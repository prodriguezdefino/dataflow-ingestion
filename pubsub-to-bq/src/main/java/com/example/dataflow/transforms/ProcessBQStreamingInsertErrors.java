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

import com.example.dataflow.utils.Utilities;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 *
 */
public class ProcessBQStreamingInsertErrors extends PTransform<PCollection<BigQueryInsertError>, PDone> {

  private final String tableSpec;
  private final TableSchema bqSchema;
  private final String recoverableMissingFieldname;
  private final String insertErrorWindowDuration;

  public ProcessBQStreamingInsertErrors(
          String tableSpec,
          TableSchema bqSchema,
          String recoverableMissingFieldname,
          String insertErrorWindowDuration) {
    this.tableSpec = tableSpec;
    this.bqSchema = bqSchema;
    this.recoverableMissingFieldname = recoverableMissingFieldname;
    this.insertErrorWindowDuration = insertErrorWindowDuration;
  }

  @Override
  public PDone expand(PCollection<BigQueryInsertError> input) {
    WriteResult errorFixResults
            = input
                    .apply(insertErrorWindowDuration + "Window",
                            Window.<BigQueryInsertError>into(
                                    FixedWindows.of(Utilities.parseDuration(insertErrorWindowDuration)))
                                    .withAllowedLateness(Utilities.parseDuration(insertErrorWindowDuration).dividedBy(4L))
                                    .discardingFiredPanes())
                    .apply("ChannelErrors", ParDo.of(new ChannelSchemaErrors(recoverableMissingFieldname)))
                    .setCoder(KvCoder.of(TableReferenceCoder.of(), TableRowJsonCoder.of()))
                    .apply("GroupByErrorType", GroupByKey.create())
                    .apply("ResolveError", ParDo.of(new FixSchemaErrors(recoverableMissingFieldname)))
                    .apply("TryAgainWriteToBQ",
                            BigQueryIO.writeTableRows()
                                    .skipInvalidRows()
                                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                    .to(tableSpec)
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
                            .to(tableSpec + "-errors")
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
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    return PDone.in(input.getPipeline());
  }

  static class ChannelSchemaErrors extends DoFn<BigQueryInsertError, KV<TableReference, TableRow>> {

    private final String recoverableMissingFieldName;

    public ChannelSchemaErrors(String recoverableMissingFieldName) {
      this.recoverableMissingFieldName = recoverableMissingFieldName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      BigQueryInsertError error = context.element();
      System.out.println(error);
      error.getError().getErrors().forEach(err -> {
        if (err.getLocation().equals(recoverableMissingFieldName)) {
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
    private final String insertTimestampFieldname;

    public FixSchemaErrors(String insertTimestampFieldname) {
      this.insertTimestampFieldname = insertTimestampFieldname;
    }

    @Setup
    public void setup() {
      // Instantiate the BQ client
      bigquery = BigQueryOptions.getDefaultInstance().getService();

    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      // Create the new field
      Field newField = Field.of(insertTimestampFieldname, StandardSQLTypeName.TIMESTAMP);

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

      if (fields.stream().noneMatch(field -> field.getName().equals(insertTimestampFieldname))) {
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
