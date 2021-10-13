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

import com.example.dataflow.utils.MetricsReporter;
import com.google.api.client.util.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Bytes;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
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
 * mvn compile exec:java -Dexec.mainClass=com.example.dataflow.PubsubToBigQuery -Dexec.cleanupDaemonThreads=false -Dexec.args=" \
 * --project=$PROJECT \
 * --avroSchemaFileLocation='avro-schema.json' \
 * --stagingLocation=gs://$STAGING_BUCKET/dataflow/staging \
 * --tempLocation=gs://$STAGING_BUCKET/dataflow/temp \
 * --numWorkers=50 \
 * --maxNumWorkers=99 \
 * --runner=DataflowRunner \
 * --workerMachineType=n1-highmem-2 \
 * --usePublicIps=false \
 * --region=us-central1 \
 * --numShards=70 \
 * --inputLocation=gs://$BUCKET/parquet/2021/10/04/17/*.parquet \
 * --outputLocation=gs://$BUCKET/csv/ \
 * --jobName='gcs-avro-to-csv' "
 * </pre>
 */
public class GCSParquetToCSV {

  private static final Logger LOG = LoggerFactory.getLogger(GCSParquetToCSV.class);

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws Exception {

    GCSParquetToCSVOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSParquetToCSVOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  static PipelineResult run(GCSParquetToCSVOptions options) throws IOException, Exception {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // read AVRO schema from local filesystem
    final String avroSchemaStr = Files.readAllLines(Paths.get(options.getAvroSchemaFileLocation()))
            .stream()
            .collect(Collectors.joining("\n"));

    LOG.info("Pipeline will be using AVRO schema:\n{}", avroSchemaStr);

    final Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
    final List<String> colNames = avroSchema.getFields().stream().map(f -> f.name()).collect(Collectors.toList());

    PCollection<KV<String, KV<String, String>>> categorized
            = pipeline.apply("MatchParquetFiles", FileIO.match().filepattern(options.getInputLocation()))
                    .apply("ReadMatches", FileIO.readMatches())
                    .apply("ReadGenericRecords", ParquetIO.readFiles(avroSchema))
                    .apply("CategorizeOnEyeColor", ParDo.of(new KVsFromEvent()));

    if (options.isUseFileIO()) {
      categorized
              .apply("WriteToCSV(FileIO.writeDynamic)",
                      FileIO.<String, KV<String, KV<String, String>>>writeDynamic()
                              // write to this output prefix
                              .to(options.getOutputLocation())
                              // using the key as the destination
                              .by(kv -> kv.getKey())
                              // with our custom sink 
                              .via(new CSVSink(colNames))
                              // with out custom filename pattern, per destination
                              .withNaming(destinationKey -> new DestinationFileNaming(destinationKey))
                              // needed since we are using a custom destination
                              .withDestinationCoder(StringUtf8Coder.of())
                              // force configured number of files per eye color (default 1)
                              .withNumShards(options.getNumShards()));
    } else {
      categorized.apply("WriteToCSV(WriteFiles+FileBasedSink)",
              WriteFiles.to(
                      new CSVFileBasedSink(
                              options.getTempLocation(),
                              new CSVDynamicDestinations(options.getOutputLocation()),
                              colNames)));
    }

    // Execute the pipeline and return the result.
    PipelineResult result = pipeline.run();

    // launch the async metrics reporter
    try ( MetricsReporter asyncReporter
            = MetricsReporter.create(
                    result, List.of(
                            MetricNameFilter.named(KVsFromEvent.class, KVsFromEvent.RECORDS_PROCESSED_COUNTER_NAME)));) {
      // wait for the pipeline to finish
      result.waitUntilFinish();

      // stop the async reporter
      asyncReporter.stopReporter();

      // Request all the metrics available for the pipeline
      MetricQueryResults metrics
              = result
                      .metrics()
                      .queryMetrics(
                              MetricsFilter.builder()
                                      .build());

      // Lets print all the available metrics
      LOG.info("***** Printing Final Values for Custom Metrics (All of them) *****");
      for (MetricResult<Long> counter : metrics.getCounters()) {
        LOG.info(counter.getName() + ":" + counter.getCommitted());
      }
    }
    return result;
  }

  static class CSVSink implements FileIO.Sink<KV<String, KV<String, String>>> {

    private final String header;
    private PrintWriter writer;

    public CSVSink(List<String> colNames) {
      this.header = Joiner.on(',').join(colNames);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer = new PrintWriter(Channels.newOutputStream(channel));
      writer.println(header);
    }

    @Override
    public void write(KV<String, KV<String, String>> element) throws IOException {
      writer.println(element.getValue().getValue());
    }

    @Override
    public void flush() throws IOException {
      writer.flush();
    }
  }

  static class CSVDynamicDestinations 
          extends FileBasedSink.DynamicDestinations<KV<String, KV<String, String>>, String, KV<String, String>> {

    private final ValueProvider<String> baseFilename;
    private Boolean windowedWrites = false;

    public CSVDynamicDestinations(ValueProvider<String> baseFilename) {
      this.baseFilename = baseFilename;
    }

    @Override
    public KV<String, String> formatRecord(KV<String, KV<String, String>> record) {
      return record.getValue();
    }

    @Override
    public String getDestination(KV<String, KV<String, String>> element) {
      return element.getKey();
    }

    @Override
    public String getDefaultDestination() {
      return "output";
    }

    @Override
    public FileBasedSink.FilenamePolicy getFilenamePolicy(String destination) {
      return DefaultFilenamePolicy.fromStandardParameters(
              ValueProvider.StaticValueProvider.of(
                      FileBasedSink.convertToFileResourceIfPossible(baseFilename.get() + destination)),
              null,
              ".csv",
              windowedWrites);
    }

  }

  public static class CSVFileBasedSink extends FileBasedSink<KV<String, KV<String, String>>, String, KV<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(CSVFileBasedSink.class);

    private final List<String> colNames;

    public CSVFileBasedSink(
            String tempDirectory,
            DynamicDestinations<KV<String, KV<String, String>>, String, KV<String, String>> dynamicDestinations,
            List<String> colNames) {
      super(ValueProvider.StaticValueProvider.of(convertToFileResourceIfPossible(tempDirectory)), dynamicDestinations);
      this.colNames = colNames;
      logger.info("tmp directory is {}", this.getTempDirectoryProvider());
    }

    private WritableByteChannelFactory shareWritableByteChannelFactory() {
      return getWritableByteChannelFactory();
    }

    @Override
    public WriteOperation<String, KV<String, String>> createWriteOperation() {
      return new CSVFileWriteOperation(this, this.getTempDirectoryProvider().get(), getDynamicDestinations(), colNames);
    }
    // WriteOperation is used to managed the output file and finalized version of the files

    private static class CSVFileWriteOperation
            extends WriteOperation<String, KV<String, String>> {

      private final List<String> colNames;

      private final DynamicDestinations<KV<String, KV<String, String>>, String, KV<String, String>> dynamicDestinations;

      public CSVFileWriteOperation(
              CSVFileBasedSink sink,
              ResourceId tempDirectory,
              DynamicDestinations<KV<String, KV<String, String>>, String, KV<String, String>> dynamicDestinations,
              List<String> colNames) {
        super(sink, tempDirectory);
        this.dynamicDestinations = dynamicDestinations;
        this.colNames = colNames;
      }

      @Override
      public Writer<String, KV<String, String>> createWriter() throws Exception {
        return new CSVFileWriter(this, dynamicDestinations, colNames);
      }
    }

    private static class CSVFileWriter extends Writer<String, KV<String, String>> {

      private WritableByteChannel dataChannel;
      private WritableByteChannel headerChannel;
      private WritableByteChannel indexChannel;
      private ResourceId headerResource;
      private ResourceId indexResource;
      private DataOutputStream dataOutputStream;
      private DataOutputStream headerOutputStream;
      private DataOutputStream indexOutputStream;
      private Long rowCounter = 0L;
      private Long indexSize = 0L;
      private final List<String> colNames;

      public CSVFileWriter(
              WriteOperation<String, KV<String, String>> writeOperation,
              DynamicDestinations<KV<String, KV<String, String>>, String, KV<String, String>> dynamicDestinations,
              List<String> colNames) {
        super(writeOperation, MimeTypes.TEXT);
        this.colNames = colNames;
        logger.info("Dynamic destination is: {}", dynamicDestinations);
      }

      @SuppressWarnings("deprecation") // uses internal test functionality.
      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        // we will need this ref to close the channel before merging files
        this.dataChannel = channel;
        // Have a way to inject the data with DataOutputStream
        dataOutputStream = new DataOutputStream(Channels.newOutputStream(channel));

        // create temp files for header and index 
        // for that we need to capture the directory and filename that is being created
        ResourceId parentDir = getOutputFile().getCurrentDirectory();
        headerResource = parentDir.resolve(
                getOutputFile().getFilename() + ".header",
                ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        headerChannel = createCompanionFileChannel(headerResource);
        headerOutputStream = new DataOutputStream(Channels.newOutputStream(headerChannel));
        // same for index file
        indexResource = parentDir.resolve(
                getOutputFile().getFilename() + ".index",
                ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        indexChannel = createCompanionFileChannel(indexResource);
        indexOutputStream = new DataOutputStream(Channels.newOutputStream(indexChannel));
      }

      private WritableByteChannel createCompanionFileChannel(ResourceId headerResource) throws Exception {
        WritableByteChannel channel = null;
        // small ugliness
        final WritableByteChannelFactory factory
                = ((CSVFileBasedSink) getWriteOperation().getSink()).shareWritableByteChannelFactory();
        // The factory may force a MIME type or it may return null, indicating to use the sink's MIME.
        String channelMimeType = MoreObjects.firstNonNull(factory.getMimeType(), MimeTypes.TEXT);
        WritableByteChannel tempChannel = FileSystems.create(headerResource, channelMimeType);
        try {
          channel = factory.create(tempChannel);
        } catch (Exception e) {
          // If we have opened the underlying channel but fail to open the compression channel,
          // we should still close the underlying channel.
          closeChannelAndThrow(tempChannel, headerResource, e);
        }
        return channel;
      }

      @Override
      public void write(KV<String, String> value) throws Exception {
        String toBeWritten;
        if (value == null) {
          toBeWritten
                  = IntStream.range(0, colNames.size())
                          .mapToObj(String::valueOf)
                          .collect(Collectors.joining(","));
        } else {
          toBeWritten = value.getValue();
        }
        toBeWritten = toBeWritten.concat("\n");
        updateIndex(dataOutputStream.size(), toBeWritten.getBytes("UTF-8").length, value.getKey());
        dataOutputStream.writeChars(toBeWritten);
        this.rowCounter++;
      }

      @Override
      protected void finishWrite() throws Exception {
        dataOutputStream.flush();
        dataOutputStream.close();
        // also close the underlying channel, since we are going to operate on this file
        dataChannel.close();
        // also close the index companion file resources
        indexOutputStream.flush();
        indexOutputStream.close();
        indexChannel.close();

        // capture header data and write it to the file: index section start, data section start
        //TODO
        String headerContent = String.format("index section size: %d, data section row count: %d", indexSize, rowCounter);
        
        // we can now close the header resources as well
        headerOutputStream.flush();
        headerOutputStream.close();
        headerChannel.close();
        
        // compose the files header first, index and data later.
        //TODO
      }

      @Override
      protected void writeHeader() throws IOException {
        this.dataOutputStream.writeChars(Joiner.on(',').join(colNames).concat("\n"));
      }

      @Override
      protected void writeFooter() throws IOException {
        this.dataOutputStream.writeChars("Footer counter is: " + this.rowCounter + "\n");
      }

      private void updateIndex(int size, int length, String key) {
        //TODO: accumulate byte size and store per key location
      }

      // Helper function to close a channel, on exception cases.
      // Always throws prior exception, with any new closing exception suppressed.
      private static void closeChannelAndThrow(
              WritableByteChannel channel, ResourceId filename, Exception prior) throws Exception {
        try {
          channel.close();
        } catch (Exception e) {
          LOG.error("Closing channel for {} failed.", filename, e);
          prior.addSuppressed(e);
        }
        // We should fail here regardless of whether above channel.close() call failed or not.
        throw prior;
      }
    }
  }

  static class KVsFromEvent extends DoFn<GenericRecord, KV<String, KV<String, String>>> {

    static final String RECORDS_PROCESSED_COUNTER_NAME = "records_processed";
    private static final Counter COUNTER = Metrics.counter(KVsFromEvent.class, RECORDS_PROCESSED_COUNTER_NAME);

    @ProcessElement
    public void processElement(ProcessContext context) {
      COUNTER.inc();
      GenericRecord gr = context.element();
      // this will only consider first level fields as columns on our CSV 
      String row = gr.getSchema()
              .getFields()
              .stream()
              .map(field -> gr.get(field.name()).toString())
              .map(r -> "\"" + r + "\"")
              .collect(Collectors.joining(","));

      // we create a KV with first key at the eye color and secondary key on the row id
      context.output(KV.of(gr.get("eyeColor").toString(), KV.of(gr.get("id").toString(), row)));
    }
  }

  static class DestinationFileNaming implements FileIO.Write.FileNaming {

    private static final Logger LOG = LoggerFactory.getLogger(DestinationFileNaming.class);

    private final String prefix;

    public DestinationFileNaming(String destinationKey) {
      this.prefix = destinationKey;
    }

    @Override
    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
      StringBuilder fileNameSB = new StringBuilder(prefix);

      if (window != null) {
        if (window instanceof GlobalWindow) {
          fileNameSB.append("-window-").append(
                  DateTime.now().toString(ISODateTimeFormat.basicDateTime()));
        } else {
          fileNameSB.append("-window-").append(
                  window.maxTimestamp().toDateTime().toString(ISODateTimeFormat.basicDateTime()));
        }
      }

      fileNameSB.append("-pane-").append(pane.getIndex())
              .append("-shard-").append(shardIndex).append("-of-").append(numShards)
              .append(".csv");

      if (!compression.equals(Compression.UNCOMPRESSED)) {
        fileNameSB.append(".").append(compression.name().toLowerCase());
      }

      LOG.debug("Windowed file name policy created: {}", fileNameSB.toString());
      return fileNameSB.toString();
    }

  }

}
