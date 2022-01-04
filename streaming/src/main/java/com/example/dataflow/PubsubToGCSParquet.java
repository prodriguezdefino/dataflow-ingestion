/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.example.dataflow;

import com.example.dataflow.transforms.WriteFormatToFileDestination;
import static com.example.dataflow.utils.Utilities.parseDuration;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into windowed Avro files at the specified output
 * directory.
 *
 * <p>
 * Output files will have the following schema:
 *
 * <pre>
 *   {
 *      "type": "record",
 *      "name": "Event"
 *      "namespace": "com.example.dataflow",
 *      "fields": [
 *          {"name": "id", "type": "string"},"
 *          {"name": "isActive", "type": "boolean"},"
 *          {"name": "balance", "type": "double"},"
 *          {"name": "picture", "type": "string"},"
 *          {"name": "age", "type": "long"},"
 *          {"name": "eyeColor", "type": "string"},"
 *          {"name": "name", "type": "string"},"
 *          {"name": "gender", "type": "string"},"
 *          {"name": "company", "type": "string"},"
 *          {"name": "email", "type": "string"},"
 *          {"name": "phone", "type": "string"},"
 *          {"name": "address", "type": "string"},"
 *          {"name": "registered", "type": "long"},"
 *          {"name": "latitude", "type": "double"},"
 *          {"name": "longitude", "type": "double"},"
 *          {"name": "tags", "type": {"type": "array", "items": "string"}},"
 *          {"name": "timestamp", "type": "long"},"
 *          {"name": "about", "type": "string"},"
 *          {"name": "about2", "type": "string"},"
 *          {"name": "about3", "type": "string"},"
 *          {"name": "about4", "type": "string"},"
 *          {"name": "about5", "type": "string"}"
 *      ]
 *   }
 * </pre>
 *
 * <p>
 * Example Usage:
 *
 * <pre>
 * mvn compile exec:java  \
 * -Dexec.mainClass=com.example.dataflow.PubsubToGCSParquet \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --avroSchemaFileLocation='avro-schema.json' \
 * --jobName='pubsubtogcsparquet' \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/temp \
 * --enableStreamingEngine \
 * --numWorkers=5 \
 * --maxNumWorkers=15 \
 * --runner=DataflowRunner \
 * --windowDuration=5m \
 * --numShards=100 \
 * --usePublicIps=false \
 * --region=${REGION} \
 * --inputSubscription=projects/${PROJECT_ID}/subscriptions/ps-to-gcs-test-sub \
 * --outputDirectory=gs://${PROJECT_ID}/parquet/ \
 * --tempDirectory=gs://${PROJECT_ID}/files-temp-dir/ \
 * "
 * </pre>
 */
public class PubsubToGCSParquet {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubToGCSParquet.class);

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws IOException {

    PStoGCSParquetOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PStoGCSParquetOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  static PipelineResult run(PStoGCSParquetOptions options) throws IOException {    
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    
    Preconditions.checkArgument(!(options.isFlatNamingStructure() && options.isHourlySuccessFiles()),
            "Flat filename and hourly filepath structure are mutually exclusive.");

    // read AVRO schema from local filesystem
    final String avroSchemaStr = Files.readAllLines(Paths.get(options.getAvroSchemaFileLocation()))
            .stream()
            .collect(Collectors.joining("\n"));

    LOG.info("Pipeline will be using AVRO schema:\n{}", avroSchemaStr);

    final Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);

    /*
     * Steps:
     *   1) Read messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed data into Parquet files.
     *   4) If configured, compose the initial # of numShards per window into bigger files.
     *   5) If configured, create SUCCESS files on empty windows or after window's writes are completed.
     */
    pipeline
            .apply("ReadPubSubEvents",
                    PubsubIO.readMessages()
                            .fromSubscription(options.getInputSubscription()))
            .apply(options.getFileWriteWindowDuration() + "Window",
                    Window.<PubsubMessage>into(
                            FixedWindows.of(parseDuration(options.getFileWriteWindowDuration())))
                            .withAllowedLateness(parseDuration(options.getFileWriteWindowDuration()).dividedBy(4L))
                            .discardingFiredPanes())
            .apply("ExtractGenericRecord",
                    ParDo.of(new PubsubMessageToArchiveDoFn(avroSchemaStr)))
            .setCoder(AvroCoder.of(avroSchema))
            .apply("WriteParquetToGCS", createWriteFormatTransform(avroSchemaStr, options));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Creates a fully configured write format instance.
   *
   * @param avroSchemaStr the avro schema to write as a String
   * @param options the pipeline options
   * @return the write instance
   */
  static WriteFormatToFileDestination<GenericRecord> createWriteFormatTransform(String avroSchemaStr, PStoGCSParquetOptions options) {
    final Boolean compressionEnabled = options.isCompressionEnabled();
    
    WriteFormatToFileDestination<GenericRecord> write
            = WriteFormatToFileDestination
                    .<GenericRecord>create()
                    .withSinkProvider(
                            () -> ParquetIO
                                    .sink(new Schema.Parser().parse(avroSchemaStr))
                                    .withCompressionCodec(
                                            compressionEnabled ? 
                                                    CompressionCodecName.SNAPPY : 
                                                    CompressionCodecName.UNCOMPRESSED))
                    .withCoder(AvroCoder.of(new Schema.Parser().parse(avroSchemaStr)))
                    .withNumShards(options.getNumShards())
                    .withOutputDirectory(options.getOutputDirectory())
                    .withOutputFilenamePrefix(options.getOutputFilenamePrefix())
                    .withOutputFilenameSuffix(options.getOutputFilenameSuffix())
                    .withTempDirectory(options.getTempDirectory())
                    .withHourlySuccessFiles(options.isHourlySuccessFiles())
                    .withSuccessFilePrefix(options.getSuccessFileNamePrefix())
                    .withFlatNamingStructure(options.isFlatNamingStructure());

    if (!options.isHourlySuccessFiles()) {
      write = write.withSuccessFilesWindowDuration(options.getFileWriteWindowDuration());
    }

    return write;
  }

  /**
   * Converts an incoming {@link PubsubMessage} to the GenericRecord class
   */
  static class PubsubMessageToArchiveDoFn extends DoFn<PubsubMessage, GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToArchiveDoFn.class);

    private DecoderFactory decoderFactory;
    private Schema schema;

    private final String avroSchemaStr;

    public PubsubMessageToArchiveDoFn(String schema) {
      this.avroSchemaStr = schema;
    }

    @Setup
    public void setupBundle() {
      decoderFactory = new DecoderFactory();
      schema = new Schema.Parser().parse(avroSchemaStr);
    }

    @ProcessElement
    public void processElement(ProcessContext context, PaneInfo pane) throws IOException {
      // capture the element, decode it from JSON into a GenericRecord and send it downstream
      PubsubMessage message = context.element();
      String msgPayload = new String(message.getPayload());
      try {
        context.output(parseGenericRecord(decoderFactory, schema, msgPayload));
      } catch (Exception e) {
        LOG.warn("Error while trying to decode pubsub message {} with schema {}", msgPayload, schema);
        LOG.error("Error while decoding the JSON message", e);
      }
    }

    static GenericRecord parseGenericRecord(DecoderFactory decoderFactory, Schema schema, String jsonMsg) throws Exception {
      JsonDecoder decoder = decoderFactory.jsonDecoder(schema, jsonMsg);
      GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
      return reader.read(null, decoder);
    }
  }

  /**
   * Beam related implementation of Parquet InputFile (copied from Beam SDK).
   */
  public static class BeamParquetInputFile implements InputFile {

    private final SeekableByteChannel seekableByteChannel;

    public BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
      this.seekableByteChannel = seekableByteChannel;
    }

    @Override
    public long getLength() throws IOException {
      return seekableByteChannel.size();
    }

    @Override
    public SeekableInputStream newStream() {
      return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {

        @Override
        public long getPos() throws IOException {
          return seekableByteChannel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
          seekableByteChannel.position(newPos);
        }
      };
    }
  }

  /**
   * Given a list of original compose part files, a destination for the compose file destination, and a ParquetIO.Sink instance it will read
   * the source files and write content to the destination.
   *
   * @param sink A ParquetIO.Sink initialized instance.
   * @param destinationPath the destination of the compose file
   * @param composeParts the original compose part files
   * @return True in case the compose file was successfully written, false otherwise.
   */
  public static Boolean composeParquetFiles(
          FileIO.Sink<GenericRecord> sink,
          String destinationPath,
          Iterable<FileIO.ReadableFile> composeParts) {

    LOG.debug("Writing into file {}", destinationPath);

    try ( WritableByteChannel writeChannel
            = FileSystems.create(
                    FileSystems.matchNewResource(destinationPath, false),
                    CreateOptions.StandardCreateOptions.builder().setMimeType("").build())) {
      sink.open(writeChannel);

      for (FileIO.ReadableFile readablePart : composeParts) {
        LOG.debug("Composing data from {}", readablePart.getMetadata().resourceId());
        AvroParquetReader.Builder<GenericRecord> readerBuilder
                = AvroParquetReader.<GenericRecord>builder(
                        new BeamParquetInputFile(readablePart.openSeekable()));
        try ( ParquetReader<GenericRecord> reader = readerBuilder.build()) {
          GenericRecord read;
          while ((read = reader.read()) != null) {
            sink.write(read);
          }
        } catch (Exception ex) {
          LOG.error("Error while composing files.", ex);
          LOG.warn("Tried to compose using file {} but failed, skipping.", readablePart.getMetadata().resourceId().getFilename());
        }
      }
      sink.flush();
      return true;
    } catch (Exception ex) {
      LOG.error("Error while composing files.", ex);
      LOG.warn("Tried to compose into file {} but failed, skipping.", destinationPath);
      return false;
    }
  }

}
