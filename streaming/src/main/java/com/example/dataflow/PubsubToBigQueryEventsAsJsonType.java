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
import com.example.dataflow.transforms.WriteToBigQuery;
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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and writes data into BQ using JSON data type for the events
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
 * --workerMachineType=n1-default-2 \
 * --usePublicIps=false \
 * --region=us-central1 \
 * --inputSubscription=$SUBSCRIPTION \
 * --outputTableSpec=$BQ_TABLE \
 * --jobName='nokill-pstobqjson'"
 * </pre>
 */
public class PubsubToBigQueryEventsAsJsonType {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubToBigQueryEventsAsJsonType.class);

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
    
    // create the avro schema object
    final Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);

    // create the JSON based BigQuery table schema
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

    // this function will be used to transform a GenericRecord into a JSON string
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

    // define a Beam Row schema to use for StorageWriteApi
    var rowSchemaWithJsonEvent = org.apache.beam.sdk.schemas.Schema
            .builder()
            .addStringField("id")
            .addDateTimeField(BQ_INSERT_TIMESTAMP_FIELDNAME)
            // will carry the event as a json string
            .addStringField("event")
            .build();

    // and a function to transform the GenericRecord into a Beam Row
    SerializableFunction<GenericRecord, Row> rowWithJsonEventMapper = record -> {
      return Row.withSchema(rowSchemaWithJsonEvent)
              .withFieldValue("id", record.get("id").toString())
              .withFieldValue(BQ_INSERT_TIMESTAMP_FIELDNAME, Instant.now())
              .withFieldValue("event", genericRecordToJsonString.apply(record))
              .build();
    };

    /*
     * Steps:
     *   1) Read messages from PubSub
     *   2) Parse those JSON message into AVRO
     *   3) Insert into BQ using Streaming API
     *   4) If configured, add an insert timestamp into the BQ table
     *   5) If configured, when the column does not exists, update the BQ table schema to add the column
     */
    pipeline
            .apply("ReadPubSubEvents",
                    PubsubIO.readMessages()
                            .fromSubscription(options.getInputSubscription()))
            .apply("ExtractGenericRecord",
                    ParDo.of(new PubsubMessageToArchiveDoFn(avroSchemaStr)))
            .setCoder(AvroCoder.of(avroSchema))
            .apply("WriteRecords",
                    WriteToBigQuery
                            .useStorageWrites(
                                    options.getOutputTableSpec(),
                                    bqTableSchema,
                                    rowWithJsonEventMapper,
                                    rowSchemaWithJsonEvent));

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

}
