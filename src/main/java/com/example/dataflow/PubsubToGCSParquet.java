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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.MoreObjects;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into windowed Avro files at the specified output
 * directory.
 *
 * <p>
 * Files output will have the following schema:
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
 * --jobName='pubsubtogcsparquet' \
 * --composeTempDirectory=gs://${PROJECT_ID}/files-temp-dir/pre-compose/ \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/temp \
 * --enableStreamingEngine \
 * --numWorkers=5 \
 * --maxNumWorkers=15 \
 * --runner=DataflowRunner \
 * --windowDuration=5m \
 * --numShards=4096 \
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

  private static final String JSON_AVRO_SCHEMA_STR = "{\n"
          + "       \"type\": \"record\",\n"
          + "       \"name\": \"Event\",\n"
          + "       \"namespace\": \"com.example.dataflow\",\n"
          + "        \"fields\": [\n"
          + "         {\"name\": \"id\", \"type\": \"string\"},\n"
          + "         {\"name\": \"isActive\", \"type\": \"boolean\"},\n"
          + "         {\"name\": \"balance\", \"type\": \"double\"},\n"
          + "         {\"name\": \"picture\", \"type\": \"string\"},\n"
          + "         {\"name\": \"age\", \"type\": \"long\"},\n"
          + "         {\"name\": \"eyeColor\", \"type\": \"string\"},\n"
          + "         {\"name\": \"name\", \"type\": \"string\"},\n"
          + "         {\"name\": \"gender\", \"type\": \"string\"},\n"
          + "         {\"name\": \"company\", \"type\": \"string\"},\n"
          + "         {\"name\": \"email\", \"type\": \"string\"},\n"
          + "         {\"name\": \"phone\", \"type\": \"string\"},\n"
          + "         {\"name\": \"address\", \"type\": \"string\"},\n"
          + "         {\"name\": \"registered\", \"type\": \"long\"},\n"
          + "         {\"name\": \"latitude\", \"type\": \"double\"},\n"
          + "         {\"name\": \"longitude\", \"type\": \"double\"},\n"
          + "         {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
          + "         {\"name\": \"timestamp\", \"type\": \"long\"},\n"
          + "         {\"name\": \"about\", \"type\": \"string\"},\n"
          + "         {\"name\": \"about2\", \"type\": \"string\"},\n"
          + "         {\"name\": \"about3\", \"type\": \"string\"},\n"
          + "         {\"name\": \"about4\", \"type\": \"string\"},\n"
          + "         {\"name\": \"about5\", \"type\": \"string\"}\n"
          + "       ]\n"
          + "    }";
  static final Schema AVRO_SCHEMA = new Schema.Parser().parse(JSON_AVRO_SCHEMA_STR);

  /**
   * Options supported by the pipeline.
   *
   * <p>
   * Inherits standard configuration options.
   */
  public interface PStoGCSParquetOptions extends DataflowPipelineOptions {

    @Description("The Cloud Pub/Sub subscription to read from.")
    @Validation.Required
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> value);

    @Description("The directory to output files to. Must end with a slash.")
    @Validation.Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @Description("The directory to output temp files to, used when composing files. Must end with a slash.")
    @Default.String("NA")
    ValueProvider<String> getComposeTempDirectory();

    void setComposeTempDirectory(ValueProvider<String> value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("prefix-name")
    ValueProvider<String> getOutputFilenamePrefix();

    void setOutputFilenamePrefix(ValueProvider<String> value);

    @Description("The suffix of the files to write.")
    @Default.String(".parquet")
    ValueProvider<String> getOutputFilenameSuffix();

    void setOutputFilenameSuffix(ValueProvider<String> value);

    @Description("The maximum number of output shards produced when writing.")
    @Default.Integer(400)
    Integer getNumShards();

    void setNumShards(Integer value);

    @Description(
            "The window duration in which data will be written. Defaults to 5m. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();

    void setWindowDuration(String value);

    @Description("The Parquet Write Temporary Directory. Must end with /")
    @Validation.Required
    ValueProvider<String> getTempDirectory();

    void setTempDirectory(ValueProvider<String> value);

    @Description("Creates a SUCCESS file once all data on a window has been received")
    @Default.Boolean(true)
    Boolean getCreateSuccessFile();

    void setCreateSuccessFile(Boolean value);

    @Description("Composes multiple small files into bigger ones (Only GCS destination)")
    @Default.Boolean(false)
    Boolean getComposeSmallFiles();

    void setComposeSmallFiles(Boolean value);

    @Description("Cleans all files part after composing them (Only GCS destination)")
    @Default.Boolean(true)
    Boolean getCleanComposePartFiles();

    void setCleanComposePartFiles(Boolean value);

  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    PStoGCSParquetOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PStoGCSParquetOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  static PipelineResult run(PStoGCSParquetOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // In order to correctly use PTransform that receives a PCollectionTuple 
    // with a GenericRecord as one of its tags we need to set a coder for it
    pipeline.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(AVRO_SCHEMA));

    // create the tuple tags for data to ingest and data signals on windows
    TupleTag<Boolean> dataOnWindowSignals = WriteFormatToGCS.dataOnWindowSignalTag();
    TupleTag<GenericRecord> dataToIngest = WriteFormatToGCS.<GenericRecord>dataToIngestTag();

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
            .apply(options.getWindowDuration() + "Window",
                    Window.<PubsubMessage>into(
                            FixedWindows.of(parseDuration(options.getWindowDuration())))
                            .withAllowedLateness(parseDuration(options.getWindowDuration()).dividedBy(4L))
                            .discardingFiredPanes())
            .apply("ExtractGenericRecord",
                    ParDo.of(
                            new PubsubMessageToArchiveDoFn(
                                    JSON_AVRO_SCHEMA_STR,
                                    dataToIngest,
                                    dataOnWindowSignals))
                            .withOutputTags(
                                    dataToIngest,
                                    TupleTagList.of(dataOnWindowSignals)))
            .apply("WriteParquetToGCS",
                    WriteFormatToGCS.<GenericRecord>create()
                            .withSink(ParquetIO
                                    .sink(AVRO_SCHEMA)
                                    .withCompressionCodec(CompressionCodecName.SNAPPY))
                            .withDataToIngestTag(dataToIngest)
                            .withDataOnWindowSignalsTag(dataOnWindowSignals)
                            .withComposeTempDirectory(options.getComposeTempDirectory())
                            .withComposeSmallFiles(options.getComposeSmallFiles())
                            .withCleanComposePartFiles(options.getCleanComposePartFiles())
                            .withNumShards(options.getNumShards())
                            .withOutputDirectory(options.getOutputDirectory())
                            .withOutputFilenamePrefix(options.getOutputFilenamePrefix())
                            .withOutputFilenameSuffix(options.getOutputFilenameSuffix())
                            .withTempDirectory(options.getTempDirectory())
                            .withWindowDuration(options.getWindowDuration()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Converts an incoming {@link PubsubMessage} to the GenericRecord class
   */
  static class PubsubMessageToArchiveDoFn extends DoFn<PubsubMessage, GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToArchiveDoFn.class);

    private DecoderFactory decoderFactory;
    private Schema schema;
    private Long countPerBundle = 0L;

    private final String avroSchemaStr;
    private final TupleTag<GenericRecord> recordsToIngest;
    private final TupleTag<Boolean> dataSignalOnWindow;

    public PubsubMessageToArchiveDoFn(String schema,
            TupleTag<GenericRecord> recordsToIngest, TupleTag<Boolean> dataSignalOnWindow) {
      this.avroSchemaStr = schema;
      this.recordsToIngest = recordsToIngest;
      this.dataSignalOnWindow = dataSignalOnWindow;
    }

    @Setup
    public void setupBundle() {
      decoderFactory = new DecoderFactory();
      schema = new Schema.Parser().parse(avroSchemaStr);
    }

    @StartBundle
    public void startBundle() {
      countPerBundle = 0L;
    }

    @ProcessElement
    public void processElement(ProcessContext context, PaneInfo pane) throws IOException {
      try {
        // capture the element, decode it from JSON into a GenericRecord and send it downstream
        PubsubMessage message = context.element();
        JsonDecoder decoder = decoderFactory.jsonDecoder(schema, new String(message.getPayload()));
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        context.output(recordsToIngest, reader.read(null, decoder));

        // In case we are producing a GenericRecord, we can check if we are in the first pane
        // of the window and propagate a signal of data in the window. 
        if (pane.isFirst() && countPerBundle == 0L) {
          countPerBundle++;
          context.output(dataSignalOnWindow, true);
        }
      } catch (IOException e) {
        LOG.error("Error while decoding the JSON message", e);
      }
    }
  }

  /**
   * Writes files in GCS using a defined format, can be configured to compose small files and create success files on windows (with or
   * without data being present).
   */
  static class WriteFormatToGCS<T> extends PTransform<PCollectionTuple, PDone> {

    private TupleTag<Boolean> dataOnWindowSignals;
    private TupleTag<T> dataToIngest;
    private FileIO.Sink<T> sink;
    private ValueProvider<String> outputFilenamePrefix;
    private ValueProvider<String> outputFilenameSuffix;
    private ValueProvider<String> tempDirectory;
    private String windowDuration;
    private Integer numShards = 400;
    private Boolean composeSmallFiles = false;
    private ValueProvider<String> composeTempDirectory;
    private ValueProvider<String> outputDirectory;
    private Boolean cleanComposePartFiles = true;
    private Boolean createSuccessFile = true;

    private WriteFormatToGCS() {
    }

    public WriteFormatToGCS<T> withOutputFilenamePrefix(ValueProvider<String> outputFilenamePrefix) {
      this.outputFilenamePrefix = outputFilenamePrefix;
      return this;
    }

    public WriteFormatToGCS<T> withOutputFilenameSuffix(ValueProvider<String> outputFilenameSuffix) {
      this.outputFilenameSuffix = outputFilenameSuffix;
      return this;
    }

    public WriteFormatToGCS<T> withTempDirectory(ValueProvider<String> tempDirectory) {
      this.tempDirectory = tempDirectory;
      return this;
    }

    public WriteFormatToGCS<T> withComposeTempDirectory(ValueProvider<String> composeTempDirectory) {
      this.composeTempDirectory = composeTempDirectory;
      return this;
    }

    public WriteFormatToGCS<T> withOutputDirectory(ValueProvider<String> outputDirectory) {
      this.outputDirectory = outputDirectory;
      return this;
    }

    public WriteFormatToGCS<T> withWindowDuration(String windowDuration) {
      this.windowDuration = windowDuration;
      return this;
    }

    public WriteFormatToGCS<T> withNumShards(Integer numShards) {
      this.numShards = numShards;
      return this;
    }

    public WriteFormatToGCS<T> withComposeSmallFiles(Boolean composeSmallFiles) {
      this.composeSmallFiles = composeSmallFiles;
      return this;
    }

    public WriteFormatToGCS<T> withCleanComposePartFiles(Boolean cleanComposePartFiles) {
      this.cleanComposePartFiles = cleanComposePartFiles;
      return this;
    }

    public WriteFormatToGCS<T> withCreateSuccessFile(Boolean createSuccessFile) {
      this.createSuccessFile = createSuccessFile;
      return this;
    }

    public WriteFormatToGCS<T> withSink(FileIO.Sink<T> sink) {
      this.sink = sink;
      return this;
    }

    public WriteFormatToGCS<T> withDataToIngestTag(TupleTag<T> dataToIngest) {
      this.dataToIngest = dataToIngest;
      return this;
    }

    public WriteFormatToGCS<T> withDataOnWindowSignalsTag(TupleTag<Boolean> dataOnWindowSignals) {
      this.dataOnWindowSignals = dataOnWindowSignals;
      return this;
    }

    public static <T> WriteFormatToGCS<T> create() {
      return new WriteFormatToGCS<>();
    }

    public static <T> TupleTag<T> dataToIngestTag() {
      return new TupleTag<T>() {
      };
    }

    public static TupleTag<Boolean> dataOnWindowSignalTag() {
      return new TupleTag<Boolean>() {
      };
    }

    @Override
    public void validate(PipelineOptions options) {
      super.validate(options);
      PStoGCSParquetOptions pstogcsOptions = (PStoGCSParquetOptions) options;
      // this Transform only supports GCS output paths for now.
      GcsPath.fromUri(pstogcsOptions.getOutputDirectory().get());

      checkArgument(outputFilenamePrefix != null, "A file prefix should be provided using with method");
      checkArgument(outputFilenameSuffix != null, "A file suffix should be provided using with method");
      checkArgument(tempDirectory != null, "A temporary directory should be provided using with method");
      checkArgument(windowDuration != null, "A window duration should be provided using withWindowDuration method");
      checkArgument(outputDirectory != null, "An output directory should be provided using with method");
      checkArgument(sink != null, "A fully configured Sink should be provided using withSink method.");
      checkArgument(dataOnWindowSignals != null && dataToIngest != null,
              "Proper TupleTags must be configured for this transform unsing with*Tag method.");
    }

    @Override
    public PDone expand(PCollectionTuple input) {
      // check if the expected tags are included in the PCollectionTuple
      if (!input.has(dataOnWindowSignals) || !input.has(dataToIngest)) {
        throw new IllegalArgumentException("Writes to GCS expects 2 tuple tags on PCollection (data to ingest and signals on windows).");
      }

      if (composeSmallFiles && composeTempDirectory.isAccessible()) {
        checkArgument(composeTempDirectory.get() != null,
                "When composing files a temp location should be configured in option --composeTempDirectory");
      }

      // create the naming strategy for created files
      WindowedFileNaming naming = new WindowedFileNaming(
              outputFilenamePrefix,
              outputFilenameSuffix,
              RandomStringUtils.randomAlphanumeric(5));

      // capture data to be ingested and send it to GCS (as final or temp yet to be determined)
      WriteFilesResult<Void> writtenFiles = input
              .get(dataToIngest)
              .apply(composeSmallFiles ? "WritePreComposeFiles" : "WriteFiles",
                      FileIO.<T>write()
                              .via(sink)
                              .withTempDirectory(tempDirectory)
                              // we will use the same naming for all writes (final or temps) to ease debugging (when needed)
                              .withNaming(naming)
                              .withNumShards(numShards)
                              // in case we are composing the files we need to send the initial writes to a temp location
                              .to(composeSmallFiles
                                      ? composeTempDirectory
                                      : outputDirectory));

      PCollection<String> fileNames = null;

      if (composeSmallFiles) {
        // we create a compose files transform
        ComposeGCSFiles<Void> composeTransform
                = ComposeGCSFiles
                        .<Void>create(
                                outputDirectory,
                                outputFilenamePrefix,
                                outputFilenameSuffix,
                                numShards)
                        .withFileNaming(naming);

        // check if we don't need to clean composing parts
        if (!cleanComposePartFiles) {
          composeTransform.withoutCleaningParts();
        }

        // remove void keys and apply compose files
        fileNames = writtenFiles
                .getPerDestinationOutputFilenames()
                //.apply("RemoveVoidKeys", Values.create())
                .apply("ComposeSmallFiles", composeTransform);
      }

      if (createSuccessFile) {
        // check if this has not being initialized
        if (fileNames == null) {
          fileNames = writtenFiles
                  .getPerDestinationOutputFilenames()
                  .apply("RemoveVoidKeys", Values.create());
        }

        // Process an empty window, in case no data is coming from pubsub
        input
                .get(dataOnWindowSignals)
                // create a SUCCESS file if the window is empty
                .apply("ProcessEmptyWindows",
                        WriteSuccessFileOnEmptyWindow.create()
                                .withOutputDirectory(outputDirectory)
                                .withNumShards(numShards)
                                .withWindowDuration(windowDuration));

        // also, process after files get writen to destination
        fileNames
                .apply("WriteSuccessFile", CreateSuccessFile.create());
      }

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * Given a String PCollection with the file names contained in a window, will wait for all of them to be completed and create a SUCCESS
   * file in the containing directory (All files are expected to be contained in the same directory).
   */
  static class CreateSuccessFile extends PTransform<PCollection<String>, PCollection<Void>> {

    private Integer numShards = 1;

    public CreateSuccessFile() {
    }

    public static CreateSuccessFile create() {
      return new CreateSuccessFile();
    }

    public CreateSuccessFile withNumShards(Integer numShards) {
      this.numShards = numShards;
      return this;
    }

    @Override
    public PCollection<Void> expand(PCollection<String> input) {

      return input
              // wait for all the files in the current window
              .apply("CombineFilesInWindow",
                      Combine.globally(CombineFilesNamesInList.create())
                              .withFanout(numShards)
                              .withoutDefaults())
              .apply("CreateSuccessFile", ParDo.of(new SuccessFileWriteDoFn()));
    }

    /**
     * Combine Strings into a list that will be used for composition on bigger files.
     */
    static class CombineFilesNamesInList extends Combine.CombineFn<String, List<String>, Iterable<String>> {

      public static CombineFilesNamesInList create() {
        return new CombineFilesNamesInList();
      }

      @Override
      public List<String> createAccumulator() {
        return new ArrayList<>();
      }

      @Override
      public List<String> addInput(List<String> mutableAccumulator, String input) {
        mutableAccumulator.add(input);
        return mutableAccumulator;
      }

      @Override
      public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
        List<String> newAccum = createAccumulator();
        for (List<String> accum : accumulators) {
          newAccum.addAll(accum);
        }
        return newAccum;
      }

      @Override
      public List<String> extractOutput(List<String> accumulator) {
        // return a consistent representation of a file list to avoid duplications when retries happens
        return accumulator
                .stream()
                .sorted()
                .collect(Collectors.toList());
      }
    }

    /**
     * Creates a SUCCESS file on the folder location of the first file in the received iterable (assumes all the files are contained in the
     * same folder).
     */
    static class SuccessFileWriteDoFn extends DoFn<Iterable<String>, Void> {

      @ProcessElement
      public void processElement(ProcessContext context) throws IOException {
        List<String> files = StreamSupport
                .stream(context.element().spliterator(), false)
                .collect(Collectors.toList());

        if (files.size() > 0) {
          createSuccessFileInPath(files.get(0), true);
          context.output((Void) null);
        }
      }
    }
  }

  /**
   * Composes a list of GCS objects (files) into bigger ones. By default all the part files are deleted, this can be disabled.
   */
  static class ComposeGCSFiles<K> extends PTransform<PCollection<KV<K, String>>, PCollection<String>> {

    // max supported by compose method in GCS
    private static final Integer MAX_FILES_IN_COMPOSE_BUNDLE = 32;

    private final ValueProvider<String> outputPath;
    private final Integer numShards;
    private Boolean cleanParts = true;
    private WindowedFileNaming naming;

    private ComposeGCSFiles(
            ValueProvider<String> outputPath,
            ValueProvider<String> filePrefix,
            ValueProvider<String> fileSuffix,
            Integer numShards) {
      this.outputPath = outputPath;
      this.naming = new WindowedFileNaming(filePrefix, fileSuffix, RandomStringUtils.random(5));
      this.numShards = numShards;
    }

    static public <K> ComposeGCSFiles<K> create(
            ValueProvider<String> outputPath,
            ValueProvider<String> filePrefix,
            ValueProvider<String> fileSuffix,
            Integer numShards) {
      return new ComposeGCSFiles<>(outputPath, filePrefix, fileSuffix, numShards);
    }

    public ComposeGCSFiles<K> withoutCleaningParts() {
      this.cleanParts = false;
      return this;
    }

    public ComposeGCSFiles<K> withFileNaming(WindowedFileNaming naming) {
      this.naming = naming;
      return this;
    }

    @Override
    public PCollection<String> expand(PCollection<KV<K, String>> input) {
      return input
              .apply("GroupIntoBatches", GroupIntoBatches.ofSize(MAX_FILES_IN_COMPOSE_BUNDLE))
              // create the file bundles that will compone the composed files
              .apply("CreateComposeBundles", ParDo.of(new CreateComposeBundles<>(naming, numShards)))
              // materialize this results, making bundles stable
              .apply("ReshuffleBundles", Reshuffle.<ComposeContext>viaRandomKey())
              // create the composed temp files
              .apply("ComposeTemporaryFiles", ParDo.of(new ComposeFiles()))
              // materialize the temp files, will reuse same temp files in retries later on 
              .apply("ReshuffleTemps", Reshuffle.<ComposeContext>viaRandomKey())
              // move the composed files to their destination
              .apply("CopyToDestination", ParDo.of(new CopyToDestination(outputPath, naming)))
              // materialize destination files
              .apply("ReshuffleDests", Reshuffle.<ComposeContext>viaRandomKey())
              // clean all the previous parts if configured to 
              .apply("Cleanup", ParDo.of(new CleanupFiles(cleanParts)))
              // materialize compose file results
              .apply("ReshuffleResults", Reshuffle.<String>viaRandomKey());
    }

    /**
     * Captures the information of a yet to be composed file
     */
    @DefaultCoder(AvroCoder.class)
    static class ComposeContext {

      public Integer shard;
      public Integer totalShards;
      public @Nullable
      String composedFile;
      public @Nullable
      String composedTempFile;
      public List<String> partFiles = new ArrayList<>();

      public ComposeContext() {
      }

      public ComposeContext(Integer shard, Integer totalShards, String composedFile,
              String composedTempFile, List<String> partFiles) {
        this.shard = shard;
        this.totalShards = totalShards;
        this.composedFile = composedFile;
        this.composedTempFile = composedTempFile;
        this.partFiles = partFiles;
      }

      public static ComposeContext of(Integer shard, Integer totalShards, String composedFile,
              String composedTempFile, List<String> partFiles) {
        return new ComposeContext(shard, totalShards, composedFile, composedTempFile, partFiles);
      }

      @Override
      public int hashCode() {
        int hash = 5;
        hash = 11 * hash + Objects.hashCode(this.partFiles);
        return hash;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final ComposeContext other = (ComposeContext) obj;
        return Objects.equals(this.partFiles, other.partFiles);
      }

      @Override
      public String toString() {
        return "ComposeContext{" + "shard=" + shard + ", totalShards=" + totalShards
                + ", composedFile=" + composedFile + ", composedTempFile=" + composedTempFile
                + ", partFiles=" + partFiles + '}';
      }

    }

    /**
     * Given a compose context object, if configured to, will delete all the original part files of the compose object.
     */
    static class CleanupFiles extends DoFn<ComposeContext, String> {

      private static final Logger LOG = LoggerFactory.getLogger(CleanupFiles.class);

      private Storage storage;
      private final Boolean cleanParts;

      public CleanupFiles(Boolean cleanParts) {
        this.cleanParts = cleanParts;
      }

      @StartBundle
      public void start(PipelineOptions options) {
        storage = StorageOptions.getDefaultInstance().getService();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        if (cleanParts) {
          LOG.debug("Will delete files: {}", context.element().partFiles);
          Long startTime = System.currentTimeMillis();
          storage.delete(
                  // grabs the part file paths
                  context.element().partFiles
                          .stream()
                          // parse the GCS paths
                          .map(f -> GcsPath.fromUri(f))
                          // create blob ids
                          .map(gcsPath -> BlobId.of(gcsPath.getBucket(), gcsPath.getObject()))
                          // collects them for deletion
                          .collect(Collectors.toList()));
          LOG.debug("{} part files deleted in {}ms.", context.element().partFiles.size(), System.currentTimeMillis() - startTime);

          Long tmpFileStartTime = System.currentTimeMillis();
          Optional
                  .ofNullable(context.element().composedTempFile)
                  .map(tmpFile -> GcsPath.fromUri(tmpFile))
                  .map(gcsPath -> BlobId.of(gcsPath.getBucket(), gcsPath.getObject()))
                  .ifPresent(tmpBlob -> {
                    storage.delete(tmpBlob);
                    LOG.debug("File {} deleted in {}ms.", tmpBlob.toString(), System.currentTimeMillis() - tmpFileStartTime);
                  });
        }

        context.output(context.element().composedFile);
      }
    }

    /**
     * Given a compose context object, will move the temporary compose file to its final configured destination.
     */
    static class CopyToDestination extends DoFn<ComposeContext, ComposeContext> {

      private static final Logger LOG = LoggerFactory.getLogger(CopyToDestination.class);

      private final ValueProvider<String> outputPath;
      private final WindowedFileNaming naming;
      private Storage storage;

      public CopyToDestination(ValueProvider<String> outputPath, WindowedFileNaming naming) {
        this.outputPath = outputPath;
        this.naming = naming;
      }

      @StartBundle
      public void start(PipelineOptions options) {
        storage = StorageOptions.getDefaultInstance().getService();
      }

      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window, PaneInfo pane) throws IOException {
        ComposeContext composeCtx = context.element();
        LOG.debug("received {} compose file context", composeCtx);

        Long startTime = System.currentTimeMillis();

        String fileDestination = outputPath.get()
                + naming.getFilename(
                        window,
                        pane,
                        context.element().totalShards,
                        context.element().shard,
                        Compression.UNCOMPRESSED);

        Blob sourceBlob = Optional.ofNullable(composeCtx.composedTempFile)
                .map(tempSourceFile -> GcsPath.fromUri(tempSourceFile))
                .map(sourcePath -> BlobId.of(sourcePath.getBucket(), sourcePath.getObject()))
                .map(tempBlob -> storage.get(tempBlob))
                .orElse(null);

        if (sourceBlob != null) {
          GcsPath destinationPath = GcsPath.fromUri(fileDestination);

          Storage.CopyRequest copyReq = Storage.CopyRequest.of(
                  sourceBlob.getBlobId(),
                  BlobId.of(destinationPath.getBucket(), destinationPath.getObject()));

          Blob destinationObj = storage.copy(copyReq).getResult();
          LOG.debug("Composed temp file {} copied to {}, composed files {}",
                  composeCtx.composedTempFile,
                  destinationObj.getSelfLink(),
                  composeCtx.partFiles);

          LOG.debug("Copied temp file in {}ms", System.currentTimeMillis() - startTime);

          context.output(
                  ComposeContext.of(composeCtx.shard,
                          composeCtx.totalShards,
                          fileDestination,
                          composeCtx.composedTempFile,
                          composeCtx.partFiles));
        } else {
          LOG.warn("Composed source not found (possible deletion upstream) {}, skipping.", composeCtx.composedTempFile);
          context.output(
                  ComposeContext.of(composeCtx.shard,
                          composeCtx.totalShards,
                          null,
                          composeCtx.composedTempFile,
                          composeCtx.partFiles));
        }
      }
    }

    /**
     * Given a KV of destinations and strings iterable, it will create the bundles that will be composed, the compose files won't have more
     * than 32 parts (current GCS operation limit).
     */
    static class CreateComposeBundles<K> extends DoFn<KV<K, Iterable<String>>, ComposeContext> {

      private static final Logger LOG = LoggerFactory.getLogger(CreateComposeBundles.class);

      private final WindowedFileNaming naming;
      private final Integer totalBundles;
      private Storage storage;
      private String bucketName;
      private String tempPathName;

      @StateId("currentNumShard")
      private final StateSpec<ValueState<Integer>> currentNumShard = StateSpecs.value();

      public CreateComposeBundles(WindowedFileNaming naming, Integer numShards) {
        this.naming = naming;
        this.totalBundles = numShards % MAX_FILES_IN_COMPOSE_BUNDLE == 0
                ? numShards / MAX_FILES_IN_COMPOSE_BUNDLE
                : (numShards / MAX_FILES_IN_COMPOSE_BUNDLE) + 1;
      }

      @StartBundle
      public void start(PipelineOptions options) {
        storage = StorageOptions.getDefaultInstance().getService();
        GcsPath gcsPath = GcsPath.fromUri(options.getTempLocation());
        bucketName = gcsPath.getBucket();
        tempPathName = gcsPath.getObject().endsWith("/") ? gcsPath.getObject() : gcsPath.getObject()
                + "/" + options.getJobName() + "/";
      }

      @ProcessElement
      public void processElement(
              @StateId("currentNumShard") ValueState<Integer> state,
              ProcessContext context,
              BoundedWindow window,
              PaneInfo pane) throws IOException {
        // Grab current shard number from state
        int currentShard = MoreObjects.firstNonNull(state.read(), 1);

        String tempFilePartialFileName
                = tempPathName + naming.getFilename(window, pane, totalBundles, currentShard, Compression.UNCOMPRESSED);
        createIfNotExistsTempComposeTarget(storage, bucketName, tempFilePartialFileName, currentShard);

        List<String> composeParts = StreamSupport
                .stream(context.element().getValue().spliterator(), false)
                .collect(Collectors.toList());

        LOG.debug("Will compose {} parts into {}.", composeParts.size(), tempFilePartialFileName);

        context.output(
                ComposeContext.of(
                        currentShard,
                        totalBundles,
                        null,
                        tempFilePartialFileName,
                        composeParts));

        // Update the state.
        state.write(currentShard + 1);
      }

      static void createIfNotExistsTempComposeTarget(Storage storage, String bucketName, String destinationPath, Integer shard) {
        try {
          BlobId blobId = BlobId.of(bucketName, destinationPath);

          if (storage.get(blobId) == null) {
            storage.create(BlobInfo
                    .newBuilder(blobId)
                    .build());
            LOG.debug("Created temp file for composition {}", blobId);
          }
        } catch (StorageException e) {
          LOG.error("Error while trying to create temp compose file " + destinationPath, e);
          throw e;
        }
      }
    }

    /**
     * Given a compose context object will grab all the part files, extract the object names and create a compose object in a temporary
     * location.
     */
    static class ComposeFiles extends DoFn<ComposeContext, ComposeContext> {

      private static final Logger LOG = LoggerFactory.getLogger(ComposeFiles.class);
      private Storage storage;
      private String bucketName;
      private SerializableFunction<Void, FileIO.Sink<GenericRecord>> sinkProvider;

      public ComposeFiles withSinkProvider(SerializableFunction<Void, FileIO.Sink<GenericRecord>> sinkProvider) {
        this.sinkProvider = sinkProvider;
        return this;
      }

      @StartBundle
      public void start(PipelineOptions options) {
        storage = StorageOptions.getDefaultInstance().getService();
        GcsPath gcsPath = GcsPath.fromUri(options.getTempLocation());
        bucketName = gcsPath.getBucket();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws IOException {
        //processElementGCSCompose(context);
        processElementParquetCompose(context);
      }

      private void processElementGCSCompose(ProcessContext context) {
        LOG.debug("Files to compose: {}", context.element());

        Long startTime = System.currentTimeMillis();
        // capture the object names only
        List<String> files = context.element().partFiles
                .stream()
                .map(file -> GcsPath.fromUri(file).getObject())
                .collect(Collectors.toList());

        Blob composedObj = composeFiles(storage, bucketName, context.element().composedTempFile, files);
        if (composedObj != null) {
          LOG.debug("compose created in {}ms,  into gs://{}/{}",
                  System.currentTimeMillis() - startTime, composedObj.getBucket(), composedObj.getName());

          context.output(
                  ComposeContext.of(context.element().shard,
                          context.element().totalShards,
                          null,
                          String.format("gs://%s/%s", composedObj.getBucket(), composedObj.getName()),
                          context.element().partFiles));
        } else {
          context.output(
                  ComposeContext.of(context.element().shard,
                          context.element().totalShards,
                          null,
                          context.element().composedTempFile,
                          context.element().partFiles));
        }
      }

      private void processElementParquetCompose(ProcessContext context) {
        FileIO.Sink<GenericRecord> sink = this.sinkProvider.apply(null);

        composeParquetFiles(sink, context.element().composedTempFile, context.element().partFiles, Compression.SNAPPY);

      }

      private static class BeamParquetInputFile implements InputFile {

        private final SeekableByteChannel seekableByteChannel;

        BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
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

      static void composeParquetFiles(
              FileIO.Sink<GenericRecord> sink, String destinationPath, Iterable<String> composeParts, Compression compression) {

        try ( WritableByteChannel writeChannel = FileSystems.create(
                FileSystems.matchNewResource(destinationPath, false),
                CreateOptions.StandardCreateOptions.builder().setMimeType("").build())) {
          sink.open(writeChannel);

          for (String partStr : composeParts) {
            ResourceId resource = FileSystems.matchSingleFileSpec(partStr).resourceId();
            AvroParquetReader.Builder<GenericRecord> readerBuilder
                    = AvroParquetReader.<GenericRecord>builder(
                            new BeamParquetInputFile(
                                    (SeekableByteChannel) compression.readDecompressed(FileSystems.open(resource))));
            try ( ParquetReader<GenericRecord> reader = readerBuilder.build()) {
              GenericRecord read;
              while ((read = reader.read()) != null) {
                sink.write(read);
              }
            }
          }
          sink.flush();
        } catch (IOException ex) {
          throw new RuntimeException("Errors while trying to compose Parquet files.", ex);
        }
      }

      static Blob composeFiles(Storage storage, String bucketName,
              String destinationPath, Iterable<String> resourceList) {
        try {
          Blob destinationBlob = storage.get(BlobId.of(bucketName, destinationPath));
          if (destinationBlob == null) {
            // if late retrying (possible deletetion downstream)
            LOG.warn("Temp composition not found gs://{}/{}, skipping.", bucketName, destinationPath);
            return null;
          }
          LOG.debug("Retrieved temp file for composition {}", destinationBlob.getName());

          Storage.ComposeRequest request
                  = Storage.ComposeRequest.newBuilder()
                          .setTarget(destinationBlob)
                          .addSource(resourceList)
                          .setTargetOptions(Storage.BlobTargetOption.detectContentType())
                          .build();

          return storage.compose(request);
        } catch (StorageException e) {
          LOG.error("Error while trying to bundle " + destinationPath + " process list " + resourceList, e);
          throw e;
        }
      }
    }
  }

  /**
   * In charge of inspecting each window the pipeline triggers and count the events occurring on it, since the pipeline contains a side
   * input that periodically generates dummy signals, if in any window only one signal is present the pipeline has not received any data
   * from its main source.
   */
  static class WriteSuccessFileOnEmptyWindow extends PTransform<PCollection<Boolean>, PDone> {

    private String windowDuration;
    private Integer numShards;
    private ValueProvider<String> outputDirectory;

    private WriteSuccessFileOnEmptyWindow() {
    }

    public static WriteSuccessFileOnEmptyWindow create() {
      return new WriteSuccessFileOnEmptyWindow();
    }

    public WriteSuccessFileOnEmptyWindow withNumShards(Integer numShards) {
      this.numShards = numShards;
      return this;
    }

    public WriteSuccessFileOnEmptyWindow withWindowDuration(String winDurationStr) {
      this.windowDuration = winDurationStr;
      return this;
    }

    public WriteSuccessFileOnEmptyWindow withOutputDirectory(ValueProvider<String> outputDir) {
      this.outputDirectory = outputDir;
      return this;
    }

    @Override
    public void validate(PipelineOptions options) {
      super.validate(options);

      checkArgument(windowDuration != null, "A window duration should be provided using the withWindowDuration method.");
      checkArgument(outputDirectory != null, "An output directory should be provided using the withOutputDirectory method.");
      checkArgument(numShards != null, "A number of shards should be provided using the withNumShards method.");
    }

    @Override
    public PDone expand(PCollection<Boolean> input) {

      // create a dummy signal on periodic intervals using same window definition
      PCollection<Boolean> periodicSignals = input.getPipeline()
              .apply("ImpulseEvery" + windowDuration,
                      GenerateSequence.from(0l).withRate(1, parseDuration(windowDuration)))
              .apply(windowDuration + "Window",
                      Window.<Long>into(FixedWindows.of(parseDuration(windowDuration)))
                              .withAllowedLateness(parseDuration(windowDuration).dividedBy(4L))
                              .discardingFiredPanes())
              .apply("CreateDummySignal", MapElements.into(TypeDescriptors.booleans()).via(ts -> true));

      // flatten elements with the input branch (main data)
      PCollectionList
              .of(periodicSignals)
              .and(input)
              .apply("FlattenSignals", Flatten.pCollections())
              .apply("CountOnWindow",
                      Combine.globally(Count.<Boolean>combineFn())
                              .withoutDefaults()
                              .withFanout(numShards))
              .apply("CheckDummySignal", ParDo.of(new CheckDataSignalOnWindowDoFn(outputDirectory)));
      return PDone.in(input.getPipeline());
    }

    /**
     * Converts an incoming {@link PubsubMessage} to the GenericRecord class
     */
    static class CheckDataSignalOnWindowDoFn extends DoFn<Long, Void> {

      private static final Logger LOG = LoggerFactory.getLogger(CheckDataSignalOnWindowDoFn.class);

      private final ValueProvider<String> rootFileLocation;

      public CheckDataSignalOnWindowDoFn(ValueProvider<String> rootFileLocation) {
        this.rootFileLocation = rootFileLocation;
      }

      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window, PaneInfo pane) {
        LOG.debug("Found {} signals on Pane {} and Window {}.", context.element(), window.toString(), pane.toString());

        // if only the dummy signal has arrived in this window
        if (context.element() < 2) {
          String outputPath = rootFileLocation.isAccessible() ? rootFileLocation.get() : "";

          if (window instanceof IntervalWindow) {
            IntervalWindow intervalWindow = (IntervalWindow) window;
            DateTime time = intervalWindow.end().toDateTime();
            outputPath = outputPath + buildPartitionedPathFromDatetime(time);
          } else {
            outputPath = outputPath + buildPartitionedPathFromDatetime(Instant.now().toDateTime());
          }
          // remove trailing /
          outputPath = outputPath.endsWith("/") ? outputPath.substring(0, outputPath.length() - 1) : outputPath;
          LOG.debug("Will create SUCCESS file at {}", outputPath);
          createSuccessFileInPath(outputPath, false);
        }
      }
    }
  }

  static class WindowedFileNaming implements FileIO.Write.FileNaming {

    private static final Logger LOG = LoggerFactory.getLogger(WindowedFileNaming.class);

    private final ValueProvider<String> filePrefix;
    private final ValueProvider<String> fileSuffix;
    private final String exec;

    public WindowedFileNaming(ValueProvider<String> filePrefix, ValueProvider<String> fileSuffix, String exec) {
      this.filePrefix = filePrefix;
      this.fileSuffix = fileSuffix;
      this.exec = exec;
    }

    @Override
    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
      String outputPath = "";

      if (window instanceof IntervalWindow) {
        IntervalWindow intervalWindow = (IntervalWindow) window;
        DateTime time = intervalWindow.end().toDateTime();
        outputPath = buildPartitionedPathFromDatetime(time);
      }

      StringBuilder fileNameSB = new StringBuilder(outputPath);

      if (!filePrefix.get().isEmpty()) {
        fileNameSB.append(filePrefix.get());
      }

      fileNameSB.append("-pane-").append(pane.getIndex())
              .append("-shard-").append(shardIndex).append("-of-").append(numShards)
              .append("-exec-").append(exec);

      if (!fileSuffix.get().isEmpty()) {
        fileNameSB.append(fileSuffix.get());
      }

      if (!Compression.UNCOMPRESSED.equals(compression)) {
        fileNameSB.append(compression.name().toLowerCase());
      }

      LOG.debug("Windowed file name policy created: {}", fileNameSB.toString());
      return fileNameSB.toString();
    }

  }

  private static final String OUTPUT_PATH_WINDOW = "YYYY/MM/DD/HH/mm/";
  private static final DateTimeFormatter YEAR = DateTimeFormat.forPattern("YYYY");
  private static final DateTimeFormatter MONTH = DateTimeFormat.forPattern("MM");
  private static final DateTimeFormatter DAY = DateTimeFormat.forPattern("dd");
  private static final DateTimeFormatter HOUR = DateTimeFormat.forPattern("HH");
  private static final DateTimeFormatter MINUTE = DateTimeFormat.forPattern("mm");

  public static String buildPartitionedPathFromDatetime(DateTime time) {
    return OUTPUT_PATH_WINDOW
            .replace("YYYY", YEAR.print(time))
            .replace("MM", MONTH.print(time))
            .replace("DD", DAY.print(time))
            .replace("HH", HOUR.print(time))
            .replace("mm", MINUTE.print(time));
  }

  /**
   * Parses a duration from a period formatted string. Values are accepted in the following formats:
   *
   * <p>
   * Formats Ns - Seconds. Example: 5s<br>
   * Nm - Minutes. Example: 13m<br>
   * Nh - Hours. Example: 2h
   *
   * <pre>
   * parseDuration(null) = NullPointerException()
   * parseDuration("")   = Duration.standardSeconds(0)
   * parseDuration("2s") = Duration.standardSeconds(2)
   * parseDuration("5m") = Duration.standardMinutes(5)
   * parseDuration("3h") = Duration.standardHours(3)
   * </pre>
   *
   * @param value The period value to parse.
   * @return The {@link Duration} parsed from the supplied period string.
   */
  private static Duration parseDuration(String value) {
    checkNotNull(value, "The specified duration must be a non-null value!");

    PeriodParser parser = new PeriodFormatterBuilder()
            .appendSeconds()
            .appendSuffix("s")
            .appendMinutes()
            .appendSuffix("m")
            .appendHours()
            .appendSuffix("h")
            .toParser();

    MutablePeriod period = new MutablePeriod();
    parser.parseInto(period, value, 0, Locale.getDefault());

    Duration duration = period.toDurationFrom(new DateTime(0));
    checkArgument(duration.getMillis() > 0, "The window duration must be greater than 0!");

    return duration;
  }

  private static void createSuccessFileInPath(String path, boolean isFile) {
    LOG.info("received path {} and isFile {}.", path, isFile);
    ResourceId dirResourceFiles = FileSystems.matchNewResource(path, isFile).getCurrentDirectory();
    ResourceId successFile = dirResourceFiles
            .resolve("SUCCESS", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

    try ( WritableByteChannel writeChannel = FileSystems.create(successFile, MimeTypes.TEXT)) {
      writeChannel.write(ByteBuffer.wrap(" ".getBytes()));
    } catch (IOException ex) {
      LOG.error("Success file creation failed.", ex);
    }
  }

}
