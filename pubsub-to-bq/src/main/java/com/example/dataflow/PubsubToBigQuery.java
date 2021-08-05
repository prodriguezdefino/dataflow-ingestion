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

import static com.example.dataflow.utils.Utilities.parseDuration;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
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
    WriteResult bqWriteResults = pipeline
            .apply("ReadPubSubEvents",
                    PubsubIO.readMessages()
                            .fromSubscription(options.getInputSubscription()))
            .apply("ExtractGenericRecord",
                    ParDo.of(new PubsubMessageToArchiveDoFn(avroSchemaStr)))
            .setCoder(AvroCoder.of(avroSchema))
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

    WriteResult errorFixResults = bqWriteResults
            .getFailedInsertsWithErr()
            .apply(options.getInsertErrorWindowDuration() + "Window",
                    Window.<BigQueryInsertError>into(
                            FixedWindows.of(parseDuration(options.getInsertErrorWindowDuration())))
                            .withAllowedLateness(parseDuration(options.getInsertErrorWindowDuration()).dividedBy(4L))
                            .discardingFiredPanes())
            .apply("ChannelErrors", ParDo.of(new ChannelSchemaErrors()))
            .setCoder(KvCoder.of(TableReferenceCoder.of(), TableRowJsonCoder.of()))
            .apply("GroupByErrorType", GroupByKey.create())
            .apply("ResolveError", ParDo.of(new FixSchemaErrors()))
            .apply("TryAgainWriteToBQ",
                    BigQueryIO.writeTableRows()
                            .skipInvalidRows()
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .to(options.getOutputTableSpec())
                            .withSchema(bqSchema)
                            .withExtendedErrorInfo()
                            .withAutoSharding()
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    errorFixResults
            .getFailedInsertsWithErr()
            .apply("WriteToErrorTable",
                    BigQueryIO.<BigQueryInsertError>write()
                            .skipInvalidRows()
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .to(options.getOutputTableSpec() + "-errors")
                            .withSchema(new TableSchema()
                                    .setFields(
                                            Arrays.asList(
                                                    new TableFieldSchema().setName("error_message").setType("STRING"),
                                                    new TableFieldSchema().setName("failed_row").setType("STRING"))))
                            .withFormatFunction(bqError
                                    -> new TableRow()
                                    .set("error_message", bqError.getError().toString())
                                    .set("failed_row", bqError.getRow()))
                            .withExtendedErrorInfo()
                            .withAutoSharding()
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            );
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

  static class ChannelSchemaErrors extends DoFn<BigQueryInsertError, KV<TableReference, TableRow>> {

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      BigQueryInsertError error = context.element();
      error.getError().getErrors().forEach(err -> {
        if (err.getLocation().equals(BQ_INSERT_TIMESTAMP_FIELDNAME)) {
          context.output(KV.of(error.getTable(), error.getRow()));
        }
      });
    }
  }

  static class TableReferenceCoder extends AtomicCoder<TableReference> {

    private static final TableReferenceCoder INSTANCE = new TableReferenceCoder();

    private TableReferenceCoder() {
    }

    public static TableReferenceCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(TableReference value, OutputStream outStream) throws CoderException, IOException {
      StringUtf8Coder.of().encode(value.getProjectId(), outStream);
      StringUtf8Coder.of().encode(value.getDatasetId(), outStream);
      StringUtf8Coder.of().encode(value.getTableId(), outStream);
    }

    @Override
    public TableReference decode(InputStream inStream) throws CoderException, IOException {
      String projectId = StringUtf8Coder.of().decode(inStream);
      String datasetId = StringUtf8Coder.of().decode(inStream);
      String tableId = StringUtf8Coder.of().decode(inStream);
      return new TableReference()
              .setProjectId(projectId)
              .setDatasetId(datasetId)
              .setTableId(tableId);
    }
  }

  static class FixSchemaErrors extends DoFn<KV<TableReference, Iterable<TableRow>>, TableRow> {

    private BigQuery bigquery;

    @Setup
    public void setup() {
      // Instantiate the BQ client
      bigquery = BigQueryOptions.getDefaultInstance().getService();

    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      // Create the new field
      Field newField = Field.of(BQ_INSERT_TIMESTAMP_FIELDNAME, StandardSQLTypeName.TIMESTAMP);

      fixTableSchemaAddingField(context.element().getKey(), newField);
      context.element().getValue().forEach(trow -> {
        context.output(trow);
      });

    }

    private void fixTableSchemaAddingField(TableReference tableRef, Field newField) {
      // Get the table, schema and fields from the already-existing table
      Table table = bigquery.getTable(TableId.of(tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()));
      com.google.cloud.bigquery.Schema schema = table.getDefinition().getSchema();
      FieldList fields = schema.getFields();

      if (fields.stream().noneMatch(field -> field.getName().equals(BQ_INSERT_TIMESTAMP_FIELDNAME))) {
        // Create a new schema adding the current fields, plus the new one
        List<Field> field_list = new ArrayList<>();
        fields.forEach(f -> {
          field_list.add(f);
        });
        field_list.add(newField);
        com.google.cloud.bigquery.Schema newSchema = com.google.cloud.bigquery.Schema.of(field_list);

        // Update the table with the new schema
        table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build().update();
      }
    }

  }

}
