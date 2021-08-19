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
package com.example.dataflow.transforms;

import com.example.dataflow.utils.Functions.*;
import com.example.dataflow.utils.WindowedFileNaming;
import com.google.common.annotations.VisibleForTesting;
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
public class WriteFormatToFileDestination<DataT> extends PTransform<PCollection<DataT>, PDone> {

  private ValueProvider<String> outputFilenamePrefix;
  private ValueProvider<String> outputFilenameSuffix;
  private ValueProvider<String> tempDirectory;
  private String successFileWindowDuration;
  private Integer numShards = 400;
  private Integer fanoutShards = 400;
  private Boolean hourlySuccessFiles = false;
  private String successFileNamePrefix = "_SUCCESS";
  private Boolean flatNamingStructure = false;
  private ValueProvider<String> outputDirectory;
  private Boolean createSuccessFile = true;
  private SerializableProvider<FileIO.Sink<DataT>> sinkProvider;
  private Coder<DataT> coder;
  private Boolean testingSeq = false;

  private WriteFormatToFileDestination() {
  }

  @VisibleForTesting
  WriteFormatToFileDestination<DataT> withTestingSeq() {
    this.testingSeq = true;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withSuccessFilePrefix(String prefix) {
    this.successFileNamePrefix = prefix;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withFlatNamingStructure(Boolean flatStructure) {
    this.flatNamingStructure = flatStructure;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withOutputFilenamePrefix(ValueProvider<String> outputFilenamePrefix) {
    this.outputFilenamePrefix = outputFilenamePrefix;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withOutputFilenameSuffix(ValueProvider<String> outputFilenameSuffix) {
    this.outputFilenameSuffix = outputFilenameSuffix;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withTempDirectory(ValueProvider<String> tempDirectory) {
    this.tempDirectory = tempDirectory;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withOutputDirectory(ValueProvider<String> outputDirectory) {
    this.outputDirectory = outputDirectory;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withSuccessFilesWindowDuration(String duration) {
    this.successFileWindowDuration = duration;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withHourlySuccessFiles(Boolean value) {
    this.hourlySuccessFiles = value;
    if (value) {
      this.successFileWindowDuration = "60m";
    }
    return this;
  }

  public WriteFormatToFileDestination<DataT> withNumShards(Integer numShards) {
    this.numShards = numShards;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withFanoutShards(Integer fanoutShards) {
    this.fanoutShards = fanoutShards;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withCreateSuccessFile(Boolean createSuccessFile) {
    this.createSuccessFile = createSuccessFile;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withSinkProvider(SerializableProvider<FileIO.Sink<DataT>> sinkProvider) {
    this.sinkProvider = sinkProvider;
    return this;
  }

  public WriteFormatToFileDestination<DataT> withCoder(Coder<DataT> coder) {
    this.coder = coder;
    return this;
  }

  public static <DataT> WriteFormatToFileDestination<DataT> create() {
    return new WriteFormatToFileDestination<>();
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
    // create the naming strategy for created files
    WindowedFileNaming naming = new WindowedFileNaming(
            outputFilenamePrefix,
            outputFilenameSuffix,
            RandomStringUtils.randomAlphanumeric(5));

    if (flatNamingStructure) {
      naming = naming.withFlatNamingStructure();
    }

    if (hourlySuccessFiles) {
      naming = naming.withHourlyPathNamingStructure();
    } else {
      checkArgument(successFileWindowDuration != null && !successFileWindowDuration.isEmpty(),
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
                    .apply("WriteFiles",
                            FileIO.<DataT>write()
                                    .via(sinkProvider.apply())
                                    .withTempDirectory(tempDirectory)
                                    // we will use the same naming for all writes (final or temps) to ease debugging (when needed)
                                    .withNaming(naming)
                                    .withNumShards(numShards)
                                    .to(outputDirectory));

    PCollection<String> fileNames = null;

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

      CreateSuccessFiles createSuccessFiles
              = CreateSuccessFiles.create()
                      .withDataOnWindowSignalsTag(dataOnWindowSignalsTag)
                      .withProcessedDataTag(processedDataTag)
                      .withFanoutShards(fanoutShards)
                      .withSuccessFileWindowDuration(successFileWindowDuration)
                      .withOutputDirectory(outputDirectory)
                      .withFlatNamingStructure(flatNamingStructure)
                      .withSuccessFilePrefix(successFileNamePrefix);

      if (testingSeq) {
        createSuccessFiles = createSuccessFiles.withTestingSeq();
      }

      // Create SUCCESS files for empty of populated windows.
      createSuccessFileInputData.apply("CreateSuccessFiles", createSuccessFiles);
    }

    return PDone.in(input.getPipeline());
  }

  static class CaptureDataOnWindowSignals<DataT> extends DoFn<DataT, DataT> {

    private final TupleTag<Boolean> dataOnWindowSignals;
    private final TupleTag<DataT> dataToBeProcessed;
    private Long countPerBundle = 0L;

    public static <DataT> TupleTag<DataT> createDataToBeProcessedTag() {
      return new TupleTag<DataT>() {
      };
    }

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

      // In case we are producing an output, we can check if we are in the first pane
      // of the window and propagate a signal of data in the window. 
      if (pane.isFirst() && countPerBundle == 0L) {
        countPerBundle++;
        context.output(dataOnWindowSignals, true);
      }
    }

  }
}
