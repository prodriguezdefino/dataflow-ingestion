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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

/** */
public class ProcessArbitraryAvroFiles {

  public interface Options extends DataflowPipelineOptions {

    @Description("Input location for the AVRO files.")
    @Validation.Required
    ValueProvider<String> getInputLocation();

    void setInputLocation(ValueProvider<String> value);

    @Description("Number of output files per original file.")
    @Default.Integer(10)
    Integer getNumShards();

    void setNumShards(Integer value);

    @Description("Output location for the processed Avro files.")
    @Validation.Required
    ValueProvider<String> getOutputLocation();

    void setOutputLocation(ValueProvider<String> value);
  }

  public static void main(String[] args) throws Exception {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(options);

    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            ElementWithSchemaAndFilename.class, ElementWithSchemaAndFilenameCoder.of());
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(ElementWithSchema.class, ElementWithSchemaCoder.of());
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(FilenameAndSchema.class, FilenameAndSchemaCoder.of());

    pipeline
        .apply("MatchInputFiles", FileIO.match().filepattern(options.getInputLocation()))
        .apply("ReadMatches", FileIO.readMatches())
        .apply(
            "ReadFilenamesWithSchemaAndRecord",
            GenericRecordAndFileInput.parseGenericRecords(
                record -> parseGenericRecord(record),
                outputParams -> createOutputElement(outputParams)))
        .apply(
            "ProcessData",
            MapElements.into(TypeDescriptor.of(ElementWithSchemaAndFilename.class)).via((kv) -> kv))
        .apply(
            "WriteToDestination",
            FileIO.<FilenameAndSchema, ElementWithSchemaAndFilename>writeDynamic()
                .by(element -> new FilenameAndSchema(element.fileName(), element.avroSchema()))
                .via(
                    Contextful.fn(element -> serializeElement(element)),
                    Contextful.fn(fileNameAndSchema -> createSink(fileNameAndSchema)))
                .to(options.getOutputLocation())
                .withNaming(
                    filenameAndSchema ->
                        FileIO.Write.defaultNaming(filenameAndSchema.fileName(), ".avsc"))
                .withNumShards(options.getNumShards()));
    
    pipeline.run();
  }

  static FileIO.Sink<GenericRecord> createSink(FilenameAndSchema fileNameAndSchema) {
    return AvroIO.<GenericRecord>sink(new Schema.Parser().parse(fileNameAndSchema.avroSchema()));
  }

  static ElementWithSchemaAndFilename createOutputElement(
      GenericRecordAndFileInput.OutputFromFileArguments<ElementWithSchema> outputParams) {
    var element = outputParams.reader().getCurrent();
    var fileName = outputParams.file().getMetadata().resourceId().getFilename();
    return new ElementWithSchemaAndFilename(
        fileName, element.avroSchema(), element.encodedGenericRecord());
  }

  static ElementWithSchema parseGenericRecord(GenericRecord record) {
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
      throw new RuntimeException(ex);
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
}
