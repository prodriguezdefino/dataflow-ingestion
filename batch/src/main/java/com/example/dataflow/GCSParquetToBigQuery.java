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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
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
 * --outputTableSpec=$BQ_TABLE \
 * --jobName='gcs-avro-to-bq' "
 * </pre>
 */
public class GCSParquetToBigQuery {

  private static final Logger LOG = LoggerFactory.getLogger(GCSParquetToBigQuery.class);

  private static final String BQ_INSERT_TIMESTAMP_FIELDNAME = "insertionTimestamp";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws IOException {

    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSParquetToBQOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  static PipelineResult run(GCSParquetToBQOptions options) throws IOException {

    // Create the pipeline
    var pipeline = Pipeline.create(options);

    // read AVRO schema from local filesystem
    var avroSchemaStr = Files.readAllLines(Paths.get(options.getAvroSchemaFileLocation()))
            .stream()
            .collect(Collectors.joining("\n"));

    LOG.info("Pipeline will be using AVRO schema:\n{}", avroSchemaStr);

    var avroSchema = new Schema.Parser().parse(avroSchemaStr);
    var beamSchema = AvroUtils.toBeamSchema(avroSchema);

    var bqSchema = BigQueryUtils.toTableSchema(beamSchema);
    SerializableFunction<GenericRecord, TableRow> bqFormatFunction
            = record -> BigQueryUtils.toTableRow(AvroUtils.toBeamRowStrict(record, null));

    if (options.isIncludeInsertTimestamp()) {
      bqSchema
              = bqSchema.set(BQ_INSERT_TIMESTAMP_FIELDNAME,
                      new TableFieldSchema()
                              .setName(BQ_INSERT_TIMESTAMP_FIELDNAME)
                              .setType("TIMESTAMP")
                              .setMode("NULLABLE"));

      bqFormatFunction = record -> {
        Row beamRow = AvroUtils.toBeamRowStrict(record, null);
        TableRow tableRow = BigQueryUtils
                .toTableRow(beamRow)
                .set(BQ_INSERT_TIMESTAMP_FIELDNAME, "AUTO");
        return tableRow;
      };
    }

    /*
     * Steps:
     *   1) Read messages from PubSub
     *   2) Parse those JSON message into AVRO
     *   3) Insert into BQ using Streaming API
     *   4) If configured, add an insert timestamp into the BQ table
     *   5) If configured, when the column does not exists, update the BQ table schema to add the column
     */
    PCollection<GenericRecord> records = pipeline
            .apply("MatchParquetFiles", FileIO.match().filepattern(options.getInputLocation()))
            .apply("ReadMatches", FileIO.readMatches())
            .apply("ReadGenericRecords", ParquetIO.readFiles(avroSchema));

    if (options.isStoreEventAsJson()) {
      var rowSchemaWithJsonEvent = org.apache.beam.sdk.schemas.Schema
              .builder()
              .addStringField("id")
              .addDateTimeField(BQ_INSERT_TIMESTAMP_FIELDNAME)
              // will host the event as a json string
              .addStringField("event")
              .build();

      SerializableFunction<GenericRecord, Row> rowWithJsonEventMapper = (GenericRecord record) -> {
        var jsonWriter = new GenericDatumWriter<>(record.getSchema());
        try ( var baos = new ByteArrayOutputStream()) {
          var encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
          jsonWriter.write(record, encoder);
          encoder.flush();
          return Row.withSchema(rowSchemaWithJsonEvent)
                  .withFieldValue("id", record.get("id").toString())
                  .withFieldValue(BQ_INSERT_TIMESTAMP_FIELDNAME, Instant.now())
                  .withFieldValue("event", baos.toString())
                  .build();
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      };

      records
              .apply("TransformToRows",
                      MapElements
                              .into(TypeDescriptors.rows())
                              .via(rowWithJsonEventMapper))
              .setCoder(RowCoder.of(rowSchemaWithJsonEvent))
              .apply("WriteToBQ",
                      BigQueryIO
                              .<Row>write()
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                              .withSchemaUpdateOptions(
                                      Sets.newHashSet(
                                              BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                                              BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION))
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                              .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                              .withTimePartitioning(
                                      new TimePartitioning()
                                              .setField(BQ_INSERT_TIMESTAMP_FIELDNAME)
                                              .setType("DAY"))
                              .to(options.getOutputTableSpec())
                              .withSchema(
                                      BigQueryUtils.toTableSchema(rowSchemaWithJsonEvent))
                              .useBeamSchema());

    } else if (options.isBatchUpload()) {
      var timestampedSchema = addNullableTimestampFieldToAvroSchema(avroSchema, BQ_INSERT_TIMESTAMP_FIELDNAME);
      var timestampedSchemaStr = timestampedSchema.toString();
      records
              .apply("AddTSToRecord", ParDo.of(new AddInsertTimestampToGenericRecord(timestampedSchemaStr)))
              .setCoder(AvroGenericCoder.of(timestampedSchema))
              .apply("WriteToBQ",
                      BigQueryIO.<GenericRecord>write()
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                              .withSchemaUpdateOptions(
                                      Sets.newHashSet(
                                              BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                                              BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION))
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                              .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                              .withTimePartitioning(
                                      new TimePartitioning()
                                              .setField(BQ_INSERT_TIMESTAMP_FIELDNAME)
                                              .setType("DAY"))
                              .to(options.getOutputTableSpec() + "$20211007")
                              // bq schema based on the new avro schema
                              .withSchema(
                                      BigQueryUtils.toTableSchema(
                                              AvroUtils.toBeamSchema(new Schema.Parser().parse(timestampedSchemaStr))))
                              // new avro schema, string format because is not serializable
                              .withAvroSchemaFactory(bqschema -> new Schema.Parser().parse(timestampedSchemaStr))
                              // simple writer configuration, not much to do
                              .withAvroWriter(
                                      avroWriteRequest -> avroWriteRequest.getElement(),
                                      schema -> new GenericDatumWriter<>(schema)));

    } else {
      records
              .apply("WriteToBQ",
                      BigQueryIO.<GenericRecord>write()
                              .skipInvalidRows()
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                              .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                              .to(options.getOutputTableSpec())
                              .withSchema(bqSchema)
                              .withFormatFunction(bqFormatFunction)
                              .withExtendedErrorInfo()
                              .withAutoSharding()
                              .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    }
    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  static class AddInsertTimestampToGenericRecord extends DoFn<GenericRecord, GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AddInsertTimestampToGenericRecord.class);

    private final String readerSchemaStr;
    private Schema readerSchema;

    public AddInsertTimestampToGenericRecord(String readerSchemaStr) {
      this.readerSchemaStr = readerSchemaStr;
    }

    @Setup
    public void setupBundle() {
      readerSchema = new Schema.Parser().parse(readerSchemaStr);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      var record = context.element();
      var writerSchema = record.getSchema();
      try {
        var w = new GenericDatumWriter<>(writerSchema);
        var outputStream = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        w.write(record, encoder);
        encoder.flush();

        var reader = new GenericDatumReader<GenericRecord>(writerSchema, readerSchema);
        var decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
        var result = reader.read(null, decoder);

        // adding current local timestamp as microseconds, supported on BQ for avro batch loads
        result.put(BQ_INSERT_TIMESTAMP_FIELDNAME, Instant.now().getMillis() * 1000);

        context.output(result);
      } catch (IOException e) {
        LOG.warn("Error while trying to add timestamp to generic record {} with schema {}", record, writerSchema);
        LOG.error("Error while adding timestamp to record", e);
      }
    }

  }

  static Schema addNullableTimestampFieldToAvroSchema(Schema base, String fieldName) {
    var timestampMilliType
            = Schema.createUnion(
                    Lists.newArrayList(
                            Schema.create(Schema.Type.NULL),
                            LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));

    var baseFields = base.getFields().stream()
            .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
            .collect(Collectors.toList());

    baseFields.add(new Schema.Field(fieldName, timestampMilliType, null, JsonProperties.NULL_VALUE));

    return Schema.createRecord(
            base.getName(),
            base.getDoc(),
            base.getNamespace(),
            false,
            baseFields);
  }

}
