package com.example.dataflow.transforms;

import com.example.dataflow.utils.Functions.*;
import com.example.dataflow.utils.WindowedFileNaming;
import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang.RandomStringUtils;

/**
 * Writes files in GCS using a defined format, can be configured to compose small files and create success files on windows (with or without
 * data being present).
 *
 * @param <DataT> The data type for the ingestion format.
 */
public class WriteFormatToGCS<DataT> extends PTransform<PCollection<DataT>, PDone> {

  private ValueProvider<String> outputFilenamePrefix;
  private ValueProvider<String> outputFilenameSuffix;
  private ValueProvider<String> tempDirectory;
  private String successFileWindowDuration;
  private Integer numShards = 400;
  private Integer fanoutShards = 400;
  private Integer composeShards = 10;
  private Boolean hourlySuccessFiles = false;
  private Boolean composeSmallFiles = false;
  private ValueProvider<String> composeTempDirectory;
  private ValueProvider<String> outputDirectory;
  private Boolean cleanComposePartFiles = true;
  private Boolean createSuccessFile = true;
  private SerializableProvider<FileIO.Sink<DataT>> sinkProvider;
  private ComposeFunction<FileIO.Sink<DataT>> composeFunction;
  private Coder<DataT> coder;

  private WriteFormatToGCS() {
  }

  public WriteFormatToGCS<DataT> withOutputFilenamePrefix(ValueProvider<String> outputFilenamePrefix) {
    this.outputFilenamePrefix = outputFilenamePrefix;
    return this;
  }

  public WriteFormatToGCS<DataT> withOutputFilenameSuffix(ValueProvider<String> outputFilenameSuffix) {
    this.outputFilenameSuffix = outputFilenameSuffix;
    return this;
  }

  public WriteFormatToGCS<DataT> withTempDirectory(ValueProvider<String> tempDirectory) {
    this.tempDirectory = tempDirectory;
    return this;
  }

  public WriteFormatToGCS<DataT> withComposeTempDirectory(ValueProvider<String> composeTempDirectory) {
    this.composeTempDirectory = composeTempDirectory;
    return this;
  }

  public WriteFormatToGCS<DataT> withOutputDirectory(ValueProvider<String> outputDirectory) {
    this.outputDirectory = outputDirectory;
    return this;
  }

  public WriteFormatToGCS<DataT> withSuccessFilesWindowDuration(String duration) {
    this.successFileWindowDuration = duration;
    return this;
  }

  public WriteFormatToGCS<DataT> withHourlySuccessFiles(Boolean value) {
    this.hourlySuccessFiles = value;
    if (value) {
      this.successFileWindowDuration = "60m";
    }
    return this;
  }

  public WriteFormatToGCS<DataT> withNumShards(Integer numShards) {
    this.numShards = numShards;
    return this;
  }

  public WriteFormatToGCS<DataT> withComposeShards(Integer composeShards) {
    this.composeShards = composeShards;
    return this;
  }

  public WriteFormatToGCS<DataT> withFanoutShards(Integer fanoutShards) {
    this.fanoutShards = fanoutShards;
    return this;
  }

  public WriteFormatToGCS<DataT> withComposeSmallFiles(Boolean composeSmallFiles) {
    this.composeSmallFiles = composeSmallFiles;
    return this;
  }

  public WriteFormatToGCS<DataT> withCleanComposePartFiles(Boolean cleanComposePartFiles) {
    this.cleanComposePartFiles = cleanComposePartFiles;
    return this;
  }

  public WriteFormatToGCS<DataT> withCreateSuccessFile(Boolean createSuccessFile) {
    this.createSuccessFile = createSuccessFile;
    return this;
  }

  public WriteFormatToGCS<DataT> withSinkProvider(SerializableProvider<FileIO.Sink<DataT>> sinkProvider) {
    this.sinkProvider = sinkProvider;
    return this;
  }

  public WriteFormatToGCS<DataT> withComposeFunction(ComposeFunction<FileIO.Sink<DataT>> composeFunction) {
    this.composeFunction = composeFunction;
    return this;
  }

  public WriteFormatToGCS<DataT> withCoder(Coder<DataT> coder) {
    this.coder = coder;
    return this;
  }

  public static <DataT> WriteFormatToGCS<DataT> create() {
    return new WriteFormatToGCS<>();
  }

  public static <DataT> TupleTag<DataT> dataToIngestTag() {
    return new TupleTag<DataT>() {
    };
  }

  public static TupleTag<Boolean> dataOnWindowSignalTag() {
    return new TupleTag<Boolean>() {
    };
  }

  @Override
  public void validate(PipelineOptions options) {
    super.validate(options);

    checkArgument(outputFilenamePrefix != null, "A file prefix should be provided using with method");
    checkArgument(outputFilenameSuffix != null, "A file suffix should be provided using with method");
    checkArgument(tempDirectory != null, "A temporary directory should be provided using with method");
    checkArgument(outputDirectory != null, "An output directory should be provided using with method");
    checkArgument(sinkProvider != null,
            "A provider function returning fully configured Sink should be provided using withSinkProvider method.");
  }

  @Override
  public PDone expand(PCollection<DataT> input) {

    if (composeSmallFiles && composeTempDirectory.isAccessible()) {
      checkArgument(composeTempDirectory.get() != null,
              "When composing files a temp location should be configured in option --composeTempDirectory");
    }

    // create the naming strategy for created files
    WindowedFileNaming naming = new WindowedFileNaming(
            outputFilenamePrefix,
            outputFilenameSuffix,
            RandomStringUtils.randomAlphanumeric(5));

    if (hourlySuccessFiles) {
      naming = naming.cloneWithHourlyPaths();
    } else {
      checkArgument(successFileWindowDuration != null,
              "A window duration should be provided for success file using withSuccessFileWindowDuration method "
              + "when duration not explicitly set");
    }

    TupleTag<Boolean> dataOnWindowSignalsTag = null;
    PCollection<DataT> dataToBeWritten = null;
    PCollection<Boolean> dataOnWindowSignals = null;

    if (createSuccessFile) {
      // create the tuple tags for data to ingest and data signals on windows
      dataOnWindowSignalsTag = CreateSuccessFiles.dataOnWindowSignalTag();
      TupleTag<DataT> dataToBeIngestedTag = new TupleTag<DataT>() {
      };

      // capture the data on window signals, having two outputs sinals with data occurring and the data to be ingested
      PCollectionTuple dataSignalsCaptured
              = input.apply("CaptureDataOnWindow",
                      ParDo.of(new CaptureDataOnWindowSignals<>(dataOnWindowSignalsTag, dataToBeIngestedTag))
                              .withOutputTags(dataToBeIngestedTag, TupleTagList.of(dataOnWindowSignalsTag)));

      // initialize the references
      dataToBeWritten = dataSignalsCaptured.get(dataToBeIngestedTag);
      dataOnWindowSignals = dataSignalsCaptured.get(dataOnWindowSignalsTag);
    } else {
      dataToBeWritten = input;
    }

    if (coder != null) {
      dataToBeWritten = dataToBeWritten.setCoder(coder);
    }

    // capture data to be ingested and send it to GCS (as final or temp yet to be determined)
    WriteFilesResult<Void> writtenFiles
            = dataToBeWritten
                    .apply(composeSmallFiles ? "WritePreComposeFiles" : "WriteFiles",
                            FileIO.<DataT>write()
                                    .via(sinkProvider.apply())
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
      ComposeGCSFiles<Void, DataT> composeTransform
              = ComposeGCSFiles
                      .<Void, DataT>create()
                      .withFileNaming(naming)
                      .withFilePrefix(outputFilenamePrefix)
                      .withFileSuffix(outputFilenameSuffix)
                      .withOutputPath(outputDirectory)
                      .withComposeShards(composeShards)
                      .withSinkProvider(sinkProvider)
                      .withComposeFunction(composeFunction);

      // check if we don't need to clean composing parts
      if (!cleanComposePartFiles) {
        composeTransform.withoutCleaningParts();
      }

      fileNames = writtenFiles
              .getPerDestinationOutputFilenames()
              .apply("RemoveDestinationKeys", Values.create())
              .apply("ComposeSmallFiles", composeTransform);
    }

    if (createSuccessFile) {
      // check if this has not being initialized
      if (fileNames == null) {
        fileNames = writtenFiles
                .getPerDestinationOutputFilenames()
                .apply("RemoveVoidKeys", Values.create());
      }

      TupleTag<String> processedDataTag = CreateSuccessFiles.processedDataTag();

      PCollectionTuple createSuccessFileInputData
              = PCollectionTuple
                      .of(processedDataTag, fileNames)
                      .and(dataOnWindowSignalsTag, dataOnWindowSignals);

      // Create SUCCESS files for empty of populated windows.
      createSuccessFileInputData.apply("CreateSuccessFiles",
              CreateSuccessFiles.create()
                      .withDataOnWindowSignalsTag(dataOnWindowSignalsTag)
                      .withProcessedDataTag(processedDataTag)
                      .withFanoutShards(fanoutShards)
                      .withSuccessFileWindowDuration(successFileWindowDuration)
                      .withOutputDirectory(outputDirectory));
    }

    return PDone.in(input.getPipeline());
  }

  static class CaptureDataOnWindowSignals<DataT> extends DoFn<DataT, DataT> {

    private final TupleTag<Boolean> dataOnWindowSignals;
    private final TupleTag<DataT> dataToBeProcessed;
    private Long countPerBundle = 0L;

    public CaptureDataOnWindowSignals(TupleTag<Boolean> dataOnWindowSignals, TupleTag<DataT> dataToBeProcessed) {
      this.dataOnWindowSignals = dataOnWindowSignals;
      this.dataToBeProcessed = dataToBeProcessed;
    }

    @StartBundle
    public void startBundle() {
      countPerBundle = 0L;
    }

    @ProcessElement
    public void processElement(ProcessContext context, PaneInfo pane) throws IOException {
      context.output(dataToBeProcessed, context.element());

      // In case we are producing a GenericRecord, we can check if we are in the first pane
      // of the window and propagate a signal of data in the window. 
      if (pane.isFirst() && countPerBundle == 0L) {
        countPerBundle++;
        context.output(dataOnWindowSignals, true);
      }
    }

  }
}
