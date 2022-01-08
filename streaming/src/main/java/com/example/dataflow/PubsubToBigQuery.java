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

import com.example.dataflow.transforms.ProcessBQStreamingInsertErrors;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Duration;
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
 * --enableStreamingEngine \
 * --numWorkers=50 \
 * --maxNumWorkers=99 \
 * --runner=DataflowRunner \
 * --workerMachineType=n1-highmem-2 \
 * --windowDuration=5m \
 * --numStreamingKeys=2000 \
 * --usePublicIps=false \
 * --region=us-central1 \
 * --inputSubscription=$SUBSCRIPTION \
 * --outputTableSpec=$BQ_TABLE \
 * --jobName='nokill-rpablo-pstobqstreaming'"
 * </pre>
 */
public class PubsubToBigQuery {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubToBigQuery.class);

  private static final String BQ_INSERT_TIMESTAMP_FIELDNAME = "insertionTimestamp";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws IOException {

    PStoBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PStoBQOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  static PipelineResult run(PStoBQOptions options) throws IOException {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // read AVRO schema from local filesystem
    final String avroSchemaStr = Files.readAllLines(Paths.get(options.getAvroSchemaFileLocation()))
            .stream()
            .collect(Collectors.joining("\n"));

    LOG.info("Pipeline will be using AVRO schema:\n{}", avroSchemaStr);

    final Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
    final org.apache.beam.sdk.schemas.Schema beamSchema = AvroUtils.toBeamSchema(avroSchema);

    TableSchema bqSchema = BigQueryUtils.toTableSchema(beamSchema);
    SerializableFunction<GenericRecord, TableRow> bqFormatFunction
            = record -> BigQueryUtils.toTableRow(AvroUtils.toBeamRowStrict(record, null));

    if (options.isIncludeInsertTimestamp()) {
      List<TableFieldSchema> fields = new ArrayList<>(bqSchema.getFields());
      fields.add(new TableFieldSchema()
              .setName(BQ_INSERT_TIMESTAMP_FIELDNAME)
              .setType("TIMESTAMP")
              .setMode("NULLABLE"));
      bqSchema = bqSchema.setFields(fields);

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
            .apply("ReadPubSubEvents",
                    PubsubIO.readMessages()
                            .fromSubscription(options.getInputSubscription()))
            .apply("ExtractGenericRecord",
                    ParDo.of(new PubsubMessageToArchiveDoFn(avroSchemaStr)))
            .setCoder(AvroCoder.of(avroSchema));

    Schema timestampedSchema = addNullableTimestampFieldToAvroSchema(avroSchema, BQ_INSERT_TIMESTAMP_FIELDNAME);
    String timestampedSchemaStr = timestampedSchema.toString();

    if (BigQueryIO.Write.Method.FILE_LOADS.name().equals(options.getWriteMode())) {
      records
              .apply("AddTSToRecord", ParDo.of(new AddInsertTimestampToGenericRecord(timestampedSchemaStr)))
              .setCoder(AvroGenericCoder.of(timestampedSchema))
              .apply("WriteToBQ",
                      BigQueryIO.<GenericRecord>write()
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                              .withSchemaUpdateOptions(
                                      Sets.newHashSet(
                                              BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                                              BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION))
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                              .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                              .to(options.getOutputTableSpec())
                              // bq schema based on the new avro schema
                              .withSchema(BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(new Schema.Parser().parse(timestampedSchemaStr))))
                              // new avro schema, string format because is not serializable
                              .withAvroSchemaFactory(bqschema -> new Schema.Parser().parse(timestampedSchemaStr))
                              // simple writer configuration, not much to do
                              .withAvroWriter(
                                      avroWriteRequest -> avroWriteRequest.getElement(),
                                      schema -> new GenericDatumWriter<>(schema))
                              .withTriggeringFrequency(Duration.standardMinutes(5L))
                              .withNumFileShards(10));

    } else {
      BigQueryIO.Write<GenericRecord> write
              = BigQueryIO.<GenericRecord>write()
                      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                      .to(options.getOutputTableSpec())
                      .withSchema(bqSchema)
                      .withExtendedErrorInfo()
                      .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors());

      if (options.isIncludeInsertTimestamp()) {
        write = write.withTimePartitioning(
                new TimePartitioning()
                        .setField(BQ_INSERT_TIMESTAMP_FIELDNAME)
                        .setType("HOUR")
                        .setRequirePartitionFilter(true));
      }

      if (BigQueryIO.Write.Method.STREAMING_INSERTS.name().equals(options.getWriteMode())) {
        WriteResult bqWriteResults
                = records
                        .apply("WriteToBQStreamingInserts",
                                write
                                        .withFormatFunction(bqFormatFunction)
                                        .withAutoSharding());

        bqWriteResults
                .getFailedInsertsWithErr()
                .apply("TryFixingInsertErrors",
                        new ProcessBQStreamingInsertErrors(
                                options.getOutputTableSpec().get(),
                                bqSchema,
                                BQ_INSERT_TIMESTAMP_FIELDNAME,
                                options.getInsertErrorWindowDuration()));
      } else {
        // STORAGE_WRITE_API mode is used for BQ inserts
        // we will set the schema of the PCollection to avoid transforming the Avro records to TableRows before inserts
        var timestampedBeamSchema = AvroUtils.toBeamSchema(timestampedSchema);
        records.setSchema(
                timestampedBeamSchema,
                AvroGenericCoder.of(timestampedSchema).getEncodedTypeDescriptor(),
                PubsubToBigQuery::toRowWithInsertTimestamp,
                AvroUtils::toGenericRecord);

        records
                .apply("WriteToBQStorageWrites",
                        write
                                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                                .useBeamSchema());
      }
    }
    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  static Row toRowWithInsertTimestamp(GenericRecord record) {
    var rowBuilder
            = Row.fromRow(
                    AvroUtils.toBeamRowStrict(
                            modifyAvroRecordSchemaAddingTimestampField(record), null));

    rowBuilder.withFieldValue(BQ_INSERT_TIMESTAMP_FIELDNAME, DateTime.now());
    return rowBuilder.build();
  }

  static GenericRecord modifyAvroRecordSchemaAddingTimestampField(GenericRecord record) {
    var newSchema = addNullableTimestampFieldToAvroSchema(record.getSchema(), BQ_INSERT_TIMESTAMP_FIELDNAME);

    try {
      return changeGenericRecordSchema(record, newSchema);
    } catch (IOException ex) {
      throw new RuntimeException("Problems while trying to change the avro record schema.", ex);
    }
  }

  static GenericRecord changeGenericRecordSchema(GenericRecord record, Schema newSchema) throws IOException {
    var oldSchema = record.getSchema();
    GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<>(oldSchema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

    w.write(record, encoder);
    encoder.flush();

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(oldSchema, newSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
    return reader.read(null, decoder);
  }

  static class AddInsertTimestampToGenericRecord extends DoFn<GenericRecord, GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToArchiveDoFn.class);

    private final String newSchemaStr;
    private Schema newSchema;

    public AddInsertTimestampToGenericRecord(String newSchemaStr) {
      this.newSchemaStr = newSchemaStr;
    }

    @Setup
    public void setupBundle() {
      newSchema = new Schema.Parser().parse(newSchemaStr);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      GenericRecord record = context.element();
      Schema writerSchema = record.getSchema();
      try {
        GenericRecord result = changeGenericRecordSchema(record, newSchema);

        // adding current timestamp as microseconds, supported on BQ for avro batch loads
        result.put(BQ_INSERT_TIMESTAMP_FIELDNAME, DateTime.now().getMillis() * 1000);

        LOG.info("Generic Record with timestamp field {}", result);

        context.output(result);
      } catch (IOException e) {
        LOG.warn("Error while trying to add timestamp to generic record {} with schema {}", record, writerSchema);
        LOG.error("Error while adding timestamp to record", e);
      }
    }

  }

  static Schema addNullableTimestampFieldToAvroSchema(Schema base, String fieldName) {
    Schema timestampMilliType
            = Schema.createUnion(
                    Lists.newArrayList(
                            Schema.create(Schema.Type.NULL),
                            LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));

    List<Schema.Field> baseFields = base.getFields().stream()
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

  /**
   * Converts an incoming {@link PubsubMessage} to the GenericRecord class
   */
  static class PubsubMessageToArchiveDoFn extends DoFn<PubsubMessage, GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToArchiveDoFn.class);

    private DecoderFactory decoderFactory;
    private Schema schema;

    private final String avroSchemaStr;

    public PubsubMessageToArchiveDoFn(String schema) {
      this.avroSchemaStr = schema;
    }

    @Setup
    public void setupBundle() {
      decoderFactory = new DecoderFactory();
      schema = new Schema.Parser().parse(avroSchemaStr);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      // capture the element, decode it from JSON into a GenericRecord and send it downstream      
      PubsubMessage message = context.element();
      String msgPayload = new String(message.getPayload());
      try {
        context.output(parseGenericRecord(decoderFactory, schema, msgPayload));
      } catch (Exception e) {
        LOG.warn("Error while trying to decode pubsub message {} with schema {}", msgPayload, schema);
        LOG.error("Error while decoding the JSON message", e);
      }
    }

    static GenericRecord parseGenericRecord(DecoderFactory decoderFactory, Schema schema, String jsonMsg) throws Exception {
      JsonDecoder decoder = decoderFactory.jsonDecoder(schema, jsonMsg);
      GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
      return reader.read(null, decoder);
    }
  }

}
