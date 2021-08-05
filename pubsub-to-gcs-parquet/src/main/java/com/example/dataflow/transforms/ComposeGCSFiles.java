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
import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composes a list of GCS objects (files) into bigger ones. By default all the original part files are deleted, this can be disabled.
 */
public class ComposeGCSFiles<KeyT, DataT> extends PTransform<PCollection<String>, PCollection<String>> {

  private Integer composeShards = 10;
  private ValueProvider<String> outputPath;
  private ValueProvider<String> filePrefix;
  private ValueProvider<String> fileSuffix;
  private Boolean cleanParts = true;
  private WindowedFileNaming naming;
  private SerializableProvider<FileIO.Sink<DataT>> sinkProvider;
  private ComposeFunction<FileIO.Sink<DataT>> composeFunction;

  private ComposeGCSFiles() {
  }

  static public <KeyT, SinkType> ComposeGCSFiles<KeyT, SinkType> create() {
    return new ComposeGCSFiles<>();
  }

  public ComposeGCSFiles<KeyT, DataT> withoutCleaningParts() {
    this.cleanParts = false;
    return this;
  }

  public ComposeGCSFiles<KeyT, DataT> withFileNaming(WindowedFileNaming naming) {
    this.naming = naming;
    return this;
  }

  public ComposeGCSFiles<KeyT, DataT> withOutputPath(ValueProvider<String> outputPath) {
    this.outputPath = outputPath;
    return this;
  }

  public ComposeGCSFiles<KeyT, DataT> withFilePrefix(ValueProvider<String> filePrefix) {
    this.filePrefix = filePrefix;
    return this;
  }

  public ComposeGCSFiles<KeyT, DataT> withFileSuffix(ValueProvider<String> fileSuffix) {
    this.fileSuffix = fileSuffix;
    return this;
  }

  public ComposeGCSFiles<KeyT, DataT> withComposeShards(Integer composeShards) {
    this.composeShards = composeShards;
    return this;
  }

  public ComposeGCSFiles<KeyT, DataT> withSinkProvider(SerializableProvider<FileIO.Sink<DataT>> sinkProvider) {
    this.sinkProvider = sinkProvider;
    return this;
  }

  public ComposeGCSFiles<KeyT, DataT> withComposeFunction(ComposeFunction<FileIO.Sink<DataT>> composeFunction) {
    this.composeFunction = composeFunction;
    return this;
  }

  @Override
  public void validate(PipelineOptions options) {
    super.validate(options);

    checkArgument(sinkProvider != null,
            "A provider function returning fully configured Sink should be provided using withSinkProvider method.");
    checkArgument(composeFunction != null, "A compose function implementation should be provided.");
  }

  @SuppressWarnings("deprecation")
  @Override
  public PCollection<String> expand(PCollection<String> input) {
    // registering coder for context object
    input.getPipeline().getCoderRegistry().registerCoderForClass(ComposeContext.class, ComposeContextCoder.of());
    // create naming strategy if not provided before
    if (this.naming == null) {
      this.naming = new WindowedFileNaming(filePrefix, fileSuffix, RandomStringUtils.random(5));
    }

    return input
            // first match all the files to be processed
            .apply("MatchFiles", FileIO.matchAll()
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
            // capture readable matches
            .apply("ToReadable", FileIO.readMatches())
            // group into batches
            .apply(WithKeys.of(v -> new Random().nextInt(composeShards)))
            .setCoder(KvCoder.of(VarIntCoder.of(), ReadableFileCoder.of()))
            .apply(GroupByKey.create())
            // create the file bundles that will compone the composed files
            .apply("CreateComposeBundles", ParDo.of(
                    new CreateComposeBundles(naming, composeShards)))
            // materialize this results, making bundles stable
            .apply("ReshuffleBundles",
                    org.apache.beam.sdk.transforms.Reshuffle.<ComposeContext>viaRandomKey())
            // create the composed temp files
            .apply("ComposeTemporaryFiles",
                    ParDo.of(new ComposeFiles<DataT>()
                            .withSinkProvider(sinkProvider)
                            .withComposeFunction(composeFunction)))
            // materialize the temp files, will reuse same temp files in retries later on 
            .apply("ReshuffleTemps",
                    org.apache.beam.sdk.transforms.Reshuffle.<ComposeContext>viaRandomKey())
            // move the composed files to their destination
            .apply("CopyToDestination", ParDo.of(new CopyToDestination(outputPath, naming)))
            // materialize destination files
            .apply("ReshuffleDests",
                    org.apache.beam.sdk.transforms.Reshuffle.<ComposeContext>viaRandomKey())
            // clean all the previous parts if configured to 
            .apply("Cleanup", ParDo.of(new CleanupFiles(cleanParts)))
            // materialize compose file results
            .apply("ReshuffleResults",
                    org.apache.beam.sdk.transforms.Reshuffle.<String>viaRandomKey());
  }

  /**
   * Captures the information of a yet to be composed file
   */
  static class ComposeContext implements Serializable {

    public Integer shard;
    public Integer totalShards;
    public String composedFile;
    public String composedTempFile;
    public List<FileIO.ReadableFile> partFiles = new ArrayList<>();

    public ComposeContext() {
    }

    public ComposeContext(Integer shard, Integer totalShards, String composedFile,
            String composedTempFile, List<FileIO.ReadableFile> partFiles) {
      this.shard = shard;
      this.totalShards = totalShards;
      this.composedFile = composedFile;
      this.composedTempFile = composedTempFile;
      this.partFiles = partFiles;
    }

    public static ComposeContext of(Integer shard, Integer totalShards, String composedFile,
            String composedTempFile, List<FileIO.ReadableFile> partFiles) {
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

  static class ComposeContextCoder extends AtomicCoder<ComposeContext> {

    private static final ComposeContextCoder INSTANCE = new ComposeContextCoder();

    /**
     * Returns the instance of {@link ReadableFileCoder}.
     */
    public static ComposeContextCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ComposeContext value, OutputStream os) throws IOException {
      VarIntCoder.of().encode(value.shard, os);
      VarIntCoder.of().encode(value.totalShards, os);
      NullableCoder.of(StringUtf8Coder.of()).encode(value.composedFile, os);
      NullableCoder.of(StringUtf8Coder.of()).encode(value.composedTempFile, os);
      ListCoder.of(ReadableFileCoder.of()).encode(value.partFiles, os);
    }

    @Override
    public ComposeContext decode(InputStream is) throws IOException {
      Integer shards = VarIntCoder.of().decode(is);
      Integer totalShards = VarIntCoder.of().decode(is);
      String composedFile = NullableCoder.of(StringUtf8Coder.of()).decode(is);
      String composedTempFile = NullableCoder.of(StringUtf8Coder.of()).decode(is);
      List<FileIO.ReadableFile> parts = ListCoder.of(ReadableFileCoder.of()).decode(is);
      return ComposeContext.of(shards, totalShards, composedFile, composedTempFile, parts);
    }
  }

  /**
   * Given a compose context object, if configured to, will delete all the original part files of the compose object.
   */
  static class CleanupFiles extends DoFn<ComposeContext, String> {

    private static final Logger LOG = LoggerFactory.getLogger(CleanupFiles.class);

    private final Boolean cleanParts;

    public CleanupFiles(Boolean cleanParts) {
      this.cleanParts = cleanParts;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      if (cleanParts) {
        LOG.debug("Will delete files: {}", context.element().partFiles);
        Long startTime = System.currentTimeMillis();
        FileSystems.delete(
                // grabs the part file paths
                context.element().partFiles
                        .stream()
                        // capture resources ids
                        .map(f -> f.getMetadata().resourceId())
                        // collects them for deletion
                        .collect(Collectors.toList()),
                MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

        LOG.debug("{} part files deleted in {}ms.", context.element().partFiles.size(), System.currentTimeMillis() - startTime);

        Long tmpFileStartTime = System.currentTimeMillis();
        Optional
                .ofNullable(context.element().composedTempFile)
                .map(tmpFile -> {
                  try {
                    return FileSystems.matchSingleFileSpec(tmpFile).resourceId();
                  } catch (IOException ex) {
                    return null;
                  }
                })
                .ifPresent(tmpBlob -> {
                  try {
                    FileSystems.delete(Arrays.asList(tmpBlob));
                    LOG.debug("File {} deleted in {}ms.", tmpBlob.toString(), System.currentTimeMillis() - tmpFileStartTime);
                  } catch (IOException ex) {
                    LOG.error("File {} was not deleted.", ex);
                  }
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

    public CopyToDestination(ValueProvider<String> outputPath, WindowedFileNaming naming) {
      this.outputPath = outputPath;
      this.naming = naming;
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

      ResourceId tempResource = Optional.ofNullable(composeCtx.composedTempFile)
              .map(tmpFile -> {
                try {
                  return FileSystems.matchSingleFileSpec(tmpFile).resourceId();
                } catch (IOException ex) {
                  return null;
                }
              })
              .orElse(null);

      if (tempResource != null) {
        FileSystems.copy(Arrays.asList(tempResource), Arrays.asList(FileSystems.matchNewResource(fileDestination, false)));
        LOG.debug("Copied temp file in {}ms", System.currentTimeMillis() - startTime);
      } else {
        LOG.warn("Composed source not found (possible deletion upstream) {}, skipping.", composeCtx.composedTempFile);
      }
      context.output(
              ComposeContext.of(
                      composeCtx.shard,
                      composeCtx.totalShards,
                      fileDestination,
                      composeCtx.composedTempFile,
                      composeCtx.partFiles));
    }
  }

  /**
   * Given a KV of destinations and strings iterable, it will create the bundles that will be composed, the compose files won't have more
   * than 32 parts (current GCS operation limit).
   */
  static class CreateComposeBundles extends DoFn<KV<Integer, Iterable<FileIO.ReadableFile>>, ComposeContext> {

    private static final Logger LOG = LoggerFactory.getLogger(CreateComposeBundles.class);

    private final WindowedFileNaming naming;
    private final Integer totalBundles;
    private String tempPathName;

    public CreateComposeBundles(WindowedFileNaming naming, Integer totalBundles) {
      this.naming = naming;
      this.totalBundles = totalBundles;
    }

    @StartBundle
    public void start(PipelineOptions options) {
      tempPathName
              = (options.getTempLocation().endsWith("/") ? options.getTempLocation() : options.getTempLocation() + "/")
              + options.getJobName() + "/";
    }

    @ProcessElement
    public void processElement(
            ProcessContext context,
            BoundedWindow window,
            PaneInfo pane) throws IOException {
      Integer currentKey = context.element().getKey();

      String tempFilePartialFileName
              = tempPathName + naming.getFilename(window, pane, totalBundles, currentKey, Compression.UNCOMPRESSED);

      List<FileIO.ReadableFile> composeParts
              = StreamSupport
                      .stream(context.element().getValue().spliterator(), false)
                      .collect(Collectors.toList());

      LOG.info("For key {} will compose {} parts into {}.", currentKey, composeParts.size(), tempFilePartialFileName);

      context.output(
              ComposeContext.of(
                      currentKey,
                      totalBundles,
                      null,
                      tempFilePartialFileName,
                      composeParts));
    }
  }

  /**
   * Given a compose context object will grab all the part files, extract the object names and create a compose object in a temporary
   * location.
   */
  static class ComposeFiles<K> extends DoFn<ComposeContext, ComposeContext> {

    private static final Logger LOG = LoggerFactory.getLogger(ComposeFiles.class);
    private SerializableProvider<FileIO.Sink<K>> sinkProvider;
    private ComposeFunction<FileIO.Sink<K>> composeFunction;

    public ComposeFiles<K> withSinkProvider(SerializableProvider<FileIO.Sink<K>> sinkProvider) {
      this.sinkProvider = sinkProvider;
      return this;
    }

    public ComposeFiles<K> withComposeFunction(ComposeFunction<FileIO.Sink<K>> composeFunction) {
      this.composeFunction = composeFunction;
      return this;
    }

    @Setup
    public void setup() {
      checkArgument(composeFunction != null, "A compose function should be provided.");
      checkArgument(sinkProvider != null, "A sink provider should be provided.");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      ComposeContext composeCtx = context.element();

      Long startTime = System.currentTimeMillis();

      composeFunction.apply(
              sinkProvider.apply(),
              composeCtx.composedTempFile,
              composeCtx.partFiles);
      LOG.info("Composed temp file in {}ms", System.currentTimeMillis() - startTime);

      context.output(
              ComposeContext.of(
                      composeCtx.shard,
                      composeCtx.totalShards,
                      composeCtx.composedFile,
                      composeCtx.composedTempFile,
                      composeCtx.partFiles));
    }
  }
}
