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

import com.example.dataflow.transforms.WriteToBigQuery;
import com.example.dataflow.utils.AvroJsonLogicalType;
import com.example.dataflow.utils.Functions;
import com.example.dataflow.utils.Utilities;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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
    options.setLabels(Map.of("como", "novandar"));

    // Create the pipeline
    var pipeline = Pipeline.create(options);

    // read AVRO schema from local filesystem
    var avroSchemaStr = Files.readAllLines(Paths.get(options.getAvroSchemaFileLocation()))
            .stream()
            .collect(Collectors.joining("\n"));

    LOG.info("Pipeline will be reading files with AVRO schema:\n{}", avroSchemaStr);

    var avroSchema = new Schema.Parser().parse(avroSchemaStr);

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
      var bqTableSchema
              = new TableSchema()
                      .setFields(
                              List.of(
                                      new TableFieldSchema()
                                              .setName("id")
                                              .setType("STRING")
                                              .setMode("NULLABLE"),
                                      new TableFieldSchema()
                                              .setName(BQ_INSERT_TIMESTAMP_FIELDNAME)
                                              .setType("TIMESTAMP")
                                              .setMode("NULLABLE"),
                                      new TableFieldSchema()
                                              .setName("event")
                                              .setType("JSON")
                                              .setMode("NULLABLE")));

      SerializableFunction<GenericRecord, String> genericRecordToJsonString = record -> {
        var jsonWriter = new GenericDatumWriter<>(record.getSchema());
        try ( var baos = new ByteArrayOutputStream()) {
          var encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
          jsonWriter.write(record, encoder);
          encoder.flush();
          return baos.toString();
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      };

      if (options.isBatchUpload()) {
        //TODO: this approach is currently not working (Feb 2022), BQ load request can not map a String to JSON when reading from AVRO
        Functions.SerializableProvider<Void> avroJsonLogicalTypeRegister = () -> {

          SpecificData.get().addLogicalTypeConversion(AvroJsonLogicalType.JsonAvroConversion.get());

          LogicalTypes.register(AvroJsonLogicalType.JSON_LOGICAL_TYPE_NAME, new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType jsonLogicalType = AvroJsonLogicalType.get();

            @Override
            public LogicalType fromSchema(Schema schema) {
              return jsonLogicalType;
            }
          });
          return (Void) null;
        };

        avroJsonLogicalTypeRegister.apply();

        var avroJsonEventSchema = Schema.createRecord(
                "Event",
                "Event avro schema",
                "com.example.dataflow",
                false,
                List.of(
                        new Schema.Field(
                                "id",
                                Schema.createUnion(
                                        Lists.newArrayList(
                                                Schema.create(Schema.Type.NULL),
                                                Schema.create(Schema.Type.STRING))),
                                "identifier of the record",
                                JsonProperties.NULL_VALUE),
                        new Schema.Field(
                                BQ_INSERT_TIMESTAMP_FIELDNAME,
                                Schema.createUnion(
                                        Lists.newArrayList(
                                                Schema.create(Schema.Type.NULL),
                                                LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))),
                                null,
                                JsonProperties.NULL_VALUE),
                        new Schema.Field(
                                "event",
                                Schema.createUnion(
                                        Lists.newArrayList(
                                                Schema.create(Schema.Type.NULL),
                                                AvroJsonLogicalType.get().addToSchema(Schema.create(Schema.Type.STRING)))),
                                "record as a json string",
                                JsonProperties.NULL_VALUE)));

        var avroJsonEventSchemaStr = avroJsonEventSchema.toString();

        LOG.info("AVRO schema for JSON events:\n{}", avroJsonEventSchemaStr);

        SerializableFunction<AvroWriteRequest<GenericRecord>, GenericRecord> avroMappingFunc
                = avroWriteRequest -> {
                  var record = avroWriteRequest.getElement();
                  return new GenericRecordBuilder(new Schema.Parser().parse(avroJsonEventSchemaStr))
                          .set("id", record.get("id").toString())
                          .set(BQ_INSERT_TIMESTAMP_FIELDNAME, Instant.now().getMillis() * 1000)
                          .set("event", genericRecordToJsonString.apply(record))
                          .build();
                };

        records.apply("WriteRecords",
                WriteToBigQuery
                        .useFileLoads(
                                options.getOutputTableSpec(),
                                bqTableSchema,
                                avroJsonEventSchemaStr)
                        .withAvroMappingFunction(avroMappingFunc));

      } else {
        var rowSchemaWithJsonEvent = org.apache.beam.sdk.schemas.Schema
                .builder()
                .addStringField("id")
                .addDateTimeField(BQ_INSERT_TIMESTAMP_FIELDNAME)
                // will host the event as a json string
                .addStringField("event")
                .build();

        SerializableFunction<GenericRecord, Row> rowWithJsonEventMapper = record -> {
          return Row.withSchema(rowSchemaWithJsonEvent)
                  .withFieldValue("id", record.get("id").toString())
                  .withFieldValue(BQ_INSERT_TIMESTAMP_FIELDNAME, Instant.now())
                  .withFieldValue("event", genericRecordToJsonString.apply(record))
                  .build();
        };

        records.apply("WriteRecords",
                WriteToBigQuery
                        .useStorageWrites(
                                options.getOutputTableSpec(),
                                bqTableSchema,
                                rowWithJsonEventMapper,
                                rowSchemaWithJsonEvent));
      }
    } else {
      var timestampedSchema = Utilities.addNullableTimestampFieldToAvroSchema(avroSchema, BQ_INSERT_TIMESTAMP_FIELDNAME);
      var timestampedSchemaStr = timestampedSchema.toString();
      var bqSchema
              = Utilities.addNullableTimestampColumnToBQSchema(
                      BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(avroSchema)),
                      BQ_INSERT_TIMESTAMP_FIELDNAME);

      LOG.info("AVRO schema with timestamp {}", timestampedSchemaStr);
      LOG.info("BQ schema with timestamp {}", bqSchema.toString());

      var tsRecords = records
              .apply("AddTSToRecord",
                      ParDo.of(new AddInsertTimestampToGenericRecord(timestampedSchemaStr, BQ_INSERT_TIMESTAMP_FIELDNAME)))
              .setCoder(AvroGenericCoder.of(timestampedSchema));

      if (options.isBatchUpload()) {
        tsRecords
                .apply("WriteRecords",
                        WriteToBigQuery
                                .useFileLoads(
                                        options.getOutputTableSpec(),
                                        bqSchema,
                                        timestampedSchemaStr)
                                .withDailyPartitionColumn(BQ_INSERT_TIMESTAMP_FIELDNAME));
      } else {
        var beamRowSchema = AvroUtils.toBeamSchema(timestampedSchema);

        tsRecords.apply("WriteRecords",
                WriteToBigQuery
                        .useStorageWrites(
                                options.getOutputTableSpec(),
                                bqSchema,
                                AvroUtils.getGenericRecordToRowFunction(beamRowSchema),
                                beamRowSchema));
      }
    }
    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  static class AddInsertTimestampToGenericRecord extends DoFn<GenericRecord, GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AddInsertTimestampToGenericRecord.class);

    private final String readerSchemaStr;
    private final String fieldName;
    private Schema readerSchema;

    public AddInsertTimestampToGenericRecord(String readerSchemaStr, String fieldName) {
      this.readerSchemaStr = readerSchemaStr;
      this.fieldName = fieldName;
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

        var batchUploads = context.getPipelineOptions().as(GCSParquetToBQOptions.class).isBatchUpload();

        // adding current local timestamp as microseconds when the avro data is ingested using batchloads, as is otherwise
        result.put(fieldName, Instant.now().getMillis() * (batchUploads ? 1000 : 1));

        context.output(result);
      } catch (IOException e) {
        LOG.warn("Error while trying to add timestamp to generic record {} with schema {}", record, writerSchema);
        LOG.error("Error while adding timestamp to record", e);
      }
    }

  }
}
