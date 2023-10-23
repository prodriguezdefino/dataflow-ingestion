/*
 * Copyright (C) 2023 Google Inc.
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
package com.example.dataflow.avro;

import com.google.auto.value.AutoValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSourceTransform;
import static org.apache.beam.sdk.io.ReadAllViaFileBasedSourceTransform.DEFAULT_USES_RESHUFFLE;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * This input PTransform can be used to read Avro files without knowing the schemas of the input
 * files, read the content of the file as GenericRecords and will return the file's content as
 * key-value pairs of filenames and the parsed GenericRecords using the provided parse function.
 *
 * @param <ParseT> The parsed type for the provided function that parses GenericRecord
 * @param <OutputT> The output type for this PTransform
 */
@AutoValue
public abstract class GenericRecordAndFileInput<ParseT, OutputT>
    extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<OutputT>> {
  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;

  abstract SerializableFunction<GenericRecord, ParseT> getParseFn();

  abstract SerializableFunction<OutputFromFileArguments<ParseT>, OutputT> getOutputFn();

  abstract Coder<ParseT> getParseCoder();

  abstract Coder<OutputT> getOutputCoder();

  abstract long getDesiredBundleSizeBytes();

  public abstract Builder<ParseT, OutputT> toBuilder();

  @AutoValue.Builder
  public abstract static class Builder<ParseT, OutputT> {
    public abstract Builder<ParseT, OutputT> setParseFn(
        SerializableFunction<GenericRecord, ParseT> parseFn);

    public abstract Builder<ParseT, OutputT> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

    public abstract Builder<ParseT, OutputT> setOutputFn(
        SerializableFunction<OutputFromFileArguments<ParseT>, OutputT> outputFn);

    public abstract Builder<ParseT, OutputT> setOutputCoder(Coder<OutputT> outputCoder);

    public abstract Builder<ParseT, OutputT> setParseCoder(Coder<ParseT> parseCoder);

    public abstract GenericRecordAndFileInput<ParseT, OutputT> build();
  }

  public static <ParseT, OutputT> GenericRecordAndFileInput<ParseT, OutputT> parseGenericRecords(
      SerializableFunction<GenericRecord, ParseT> parseFn,
      Coder<ParseT> parseOutputCoder,
      SerializableFunction<OutputFromFileArguments<ParseT>, OutputT> outputFn,
      Coder<OutputT> outputCoder) {
    return new AutoValue_GenericRecordAndFileInput.Builder<ParseT, OutputT>()
        .setParseFn(parseFn)
        .setOutputFn(outputFn)
        .setParseCoder(parseOutputCoder)
        .setOutputCoder(outputCoder)
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .build();
  }

  @Override
  public PCollection<OutputT> expand(PCollection<FileIO.ReadableFile> input) {
    final SerializableFunction<GenericRecord, ParseT> parseFn = getParseFn();
    final SerializableFunction<String, FileBasedSource<ParseT>> createSource =
        new CreateParseSourceFn<>(parseFn, getParseCoder());
    return input.apply(
        "Parse Files via FileBasedSource",
        new ReadAllFilesAndRecordsViaFileBasedSource<>(
            getOutputFn(), getDesiredBundleSizeBytes(), createSource, getOutputCoder()));
  }

  static class CreateParseSourceFn<H> implements SerializableFunction<String, FileBasedSource<H>> {
    private final SerializableFunction<GenericRecord, H> parseFn;
    private final Coder<H> coder;

    CreateParseSourceFn(SerializableFunction<GenericRecord, H> parseFn, Coder<H> coder) {
      this.parseFn = parseFn;
      this.coder = coder;
    }

    @Override
    public FileBasedSource<H> apply(String input) {
      return AvroSource.from(input).withParseFn(parseFn, coder);
    }
  }

  static class ReadAllFilesAndRecordsViaFileBasedSource<S, OutputT>
      extends ReadAllViaFileBasedSourceTransform<S, OutputT> {
    private final SerializableFunction<OutputFromFileArguments<S>, OutputT> outputFn;

    public ReadAllFilesAndRecordsViaFileBasedSource(
        final SerializableFunction<OutputFromFileArguments<S>, OutputT> outputFn,
        long desiredBundleSizeBytes,
        SerializableFunction<String, ? extends FileBasedSource<S>> createSource,
        Coder<OutputT> coder) {
      super(
          desiredBundleSizeBytes,
          createSource,
          coder,
          DEFAULT_USES_RESHUFFLE,
          new ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler());
      this.outputFn = outputFn;
    }

    @Override
    protected DoFn<KV<FileIO.ReadableFile, OffsetRange>, OutputT> readRangesFn() {
      return new ReadFileRangesFn<>(outputFn, createSource, exceptionHandler);
    }

    private static class ReadFileRangesFn<K, OutputT> extends AbstractReadFileRangesFn<K, OutputT> {
      private final SerializableFunction<OutputFromFileArguments<K>, OutputT> outputFn;

      public ReadFileRangesFn(
          final SerializableFunction<OutputFromFileArguments<K>, OutputT> outputFn,
          final SerializableFunction<String, ? extends FileBasedSource<K>> createSource,
          final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler) {
        super(createSource, exceptionHandler);
        this.outputFn = outputFn;
      }

      @Override
      protected OutputT makeOutput(
          final FileIO.ReadableFile file,
          final OffsetRange range,
          final FileBasedSource<K> fileBasedSource,
          final BoundedSource.BoundedReader<K> reader) {
        return outputFn.apply(new OutputFromFileArguments<>(file, range, fileBasedSource, reader));
      }
    }
  }

  /** Data carrier for the arguments of an output construction method. */
  public record OutputFromFileArguments<ReadType>(
      FileIO.ReadableFile file,
      OffsetRange range,
      FileBasedSource<ReadType> fileBasedSource,
      BoundedSource.BoundedReader<ReadType> reader) {}
}
