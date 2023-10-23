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

import com.example.dataflow.avro.Types.ElementWithSchema;
import com.example.dataflow.avro.Types.ElementWithSchemaAndFilename;
import com.example.dataflow.avro.Types.ElementWithSchemaAndFilenameCoder;
import com.example.dataflow.avro.Types.ElementWithSchemaCoder;
import com.example.dataflow.avro.Types.FilenameAndSchema;
import com.example.dataflow.avro.Types.FilenameAndSchemaCoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

/** */
public class ProcessArbitraryAvroFiles {

  public interface Options extends DataflowPipelineOptions {

    @Description("Input location for the AVRO files.")
    @Validation.Required
    String getFilesLocation();

    void setFilesLocation(String value);

    @Description("Output location for the processed Avro files.")
    @Validation.Required
    String getOutputLocation();

    void setOutputLocation(String value);

    @Description("File size threshold we will use to decide if spliting the output files.")
    @Default.Long(37 * 1024 * 1024L)
    Long getFileSizeThreshold();

    void setFileSizeThreshold(Long size);
  }

  public static void main(String[] args) throws Exception {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(options);

    pipeline
        .getCoderRegistry()
        .registerCoderForType(
            TypeDescriptor.of(ElementWithSchemaAndFilename.class),
            ElementWithSchemaAndFilenameCoder.of());
    pipeline
        .getCoderRegistry()
        .registerCoderForType(
            TypeDescriptor.of(ElementWithSchema.class), ElementWithSchemaCoder.of());
    pipeline
        .getCoderRegistry()
        .registerCoderForType(
            TypeDescriptor.of(FilenameAndSchema.class), FilenameAndSchemaCoder.of());

    var writeBranches =
        pipeline
            .apply("MatchInputFiles", FileIO.match().filepattern(options.getFilesLocation()))
            .apply("ReadMatches", FileIO.readMatches())
            .apply(
                "ReadFilenamesWithSchemaAndRecord",
                GenericRecordAndFileInput.parseGenericRecords(
                    ProcessArbitraryAvroFiles::parseAvroToElementWithSchema,
                    ElementWithSchemaCoder.of(),
                    ProcessArbitraryAvroFiles::readOutputAsElementWithSchemaAndFilename,
                    ElementWithSchemaAndFilenameCoder.of()))
            .apply(
                "ProcessData",
                MapElements.into(TypeDescriptor.of(ElementWithSchemaAndFilename.class))
                    .via((kv) -> kv))
            .apply(
                "DecideSplit",
                ParDo.of(new DecideIfSplitNeeded(options.getFileSizeThreshold()))
                    .withOutputTags(
                        DecideIfSplitNeeded.largerThanThreshold,
                        TupleTagList.of(DecideIfSplitNeeded.smallerThanThreshold)));

    writeBranches
        .get(DecideIfSplitNeeded.largerThanThreshold)
        .apply("WriteToSplittedDestination", createWriteDestination(options, true));

    writeBranches
        .get(DecideIfSplitNeeded.smallerThanThreshold)
        .apply("WriteToDestination", createWriteDestination(options, false));

    pipeline.run();
  }

  static FileIO.Write<FilenameAndSchema, ElementWithSchemaAndFilename> createWriteDestination(
      Options options, boolean shouldSplit) {

    var write =
        FileIO.<FilenameAndSchema, ElementWithSchemaAndFilename>writeDynamic()
            .by(element -> new FilenameAndSchema(element.fileName(), element.avroSchema()))
            .via(
                Contextful.fn(element -> serializeElement(element)),
                Contextful.fn(fileNameAndSchema -> createSink(fileNameAndSchema)))
            .to(options.getOutputLocation())
            .withNaming(
                filenameAndSchema ->
                    FileIO.Write.defaultNaming(filenameAndSchema.fileName(), ".avsc"))
            .withDestinationCoder(FilenameAndSchemaCoder.of());

    if (!shouldSplit) {
      // we want the same file as input if the file is smaller then threshold.
      write = write.withNumShards(1);
    }

    return write;
  }

  static FileIO.Sink<GenericRecord> createSink(FilenameAndSchema fileNameAndSchema) {
    return AvroIO.<GenericRecord>sink(new Schema.Parser().parse(fileNameAndSchema.avroSchema()));
  }

  static ElementWithSchemaAndFilename readOutputAsElementWithSchemaAndFilename(
      GenericRecordAndFileInput.OutputFromFileArguments<ElementWithSchema> outputParams) {
    var element = outputParams.reader().getCurrent();
    var fileName = outputParams.file().getMetadata().resourceId().getFilename();
    var fileSize = outputParams.file().getMetadata().sizeBytes();
    return new ElementWithSchemaAndFilename(
        fileName, fileSize, element.avroSchema(), element.encodedGenericRecord());
  }

  static ElementWithSchema parseAvroToElementWithSchema(GenericRecord record) {
    try {
      var schema = record.getSchema();
      var outputStream = new ByteArrayOutputStream();
      var jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);
      var datumWriter = new GenericDatumWriter<GenericRecord>(record.getSchema());
      datumWriter.write(record, jsonEncoder);
      jsonEncoder.flush();
      outputStream.close();

      return new ElementWithSchema(schema.toString(), outputStream.toByteArray());
    } catch (IOException ex) {
      throw new RuntimeException("Error while encoding the read record.", ex);
    }
  }

  static GenericRecord serializeElement(ElementWithSchemaAndFilename element) {
    try {
      var schema = new Schema.Parser().parse(element.avroSchema());
      var reader = new GenericDatumReader<GenericRecord>(schema);
      return reader.read(
          null,
          DecoderFactory.get()
              .jsonDecoder(schema, new ByteArrayInputStream(element.encodedGenericRecord())));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Error deserializing json %s to Avro of schema %s",
              new ByteArrayInputStream(element.encodedGenericRecord()).toString(),
              element.avroSchema()),
          e);
    }
  }

  static class DecideIfSplitNeeded
      extends DoFn<ElementWithSchemaAndFilename, ElementWithSchemaAndFilename> {

    static final TupleTag<ElementWithSchemaAndFilename> largerThanThreshold = new TupleTag<>() {};
    static final TupleTag<ElementWithSchemaAndFilename> smallerThanThreshold = new TupleTag<>() {};

    private final long thresholdValue;

    public DecideIfSplitNeeded(long thresholdValue) {
      this.thresholdValue = thresholdValue;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      if (context.element().fileSize() >= thresholdValue) {
        context.output(largerThanThreshold, context.element());
      } else {
        context.output(smallerThanThreshold, context.element());
      }
    }
  }
}
