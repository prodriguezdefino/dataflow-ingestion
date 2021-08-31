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
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transform will try to capture the error type after a failed Streaming write into BQ and decide, based on the configured fields, if a
 * schema update is needed in order to overcome the problem. Once the schema has been updated the writes into BQ will be retried until
 * succeed. In case the errors are not schema related it will propagate them downstream for later handling.
 *
 * Is expected that the inserts with the new schema will stabilize after some secs/mins, during that time frame inserts will be retried
 * until successful.
 */
public class ProcessBQStreamingInsertErrors extends PTransform<PCollection<BigQueryInsertError>, PCollection<BigQueryInsertError>> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessBQStreamingInsertErrors.class);

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
  public PCollection<BigQueryInsertError> expand(PCollection<BigQueryInsertError> input) {
    PCollectionTuple channeledErrors = input
            .apply(insertErrorWindowDuration + "Window",
                    Window.<BigQueryInsertError>into(
                            FixedWindows.of(Utilities.parseDuration(insertErrorWindowDuration)))
                            .discardingFiredPanes())
            .apply("ChannelErrors",
                    ParDo
                            .of(new ChannelSchemaErrors(recoverableMissingFieldname))
                            .withOutputTags(ChannelSchemaErrors.RECOVERABLE_ERRORS,
                                    TupleTagList.of(ChannelSchemaErrors.NON_RECOVERABLE_ERRORS)));
    channeledErrors
            .get(ChannelSchemaErrors.RECOVERABLE_ERRORS)
            .setCoder(KvCoder.of(TableReferenceCoder.of(), TableRowJsonCoder.of()))
            .apply("GroupByErrorType", GroupByKey.create())
            .apply("ResolveError", ParDo.of(new FixSchemaErrors(recoverableMissingFieldname)))
            .apply("TryAgainWriteToBQ",
                    BigQueryIO.writeTableRows()
                            .skipInvalidRows()
                            // for those values we have not registered as part of the new schema we ignore them
                            .ignoreUnknownValues()
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .to(tableSpec)
                            .withSchema(bqSchema)
                            .withExtendedErrorInfo()
                            // We expect to have fixed the previously failed rows, that's why we force all the retries.
                            // After the retries have stabilized we should not see more data coming into this branch of the pipeline.
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry()));

    // those errors that are non recoverable we return them for DLQ processing downstream
    return channeledErrors.get(ChannelSchemaErrors.NON_RECOVERABLE_ERRORS);
  }

  static class ChannelSchemaErrors extends DoFn<BigQueryInsertError, KV<TableReference, TableRow>> {

    private static final TupleTag<BigQueryInsertError> NON_RECOVERABLE_ERRORS = new TupleTag<BigQueryInsertError>() {
    };
    private static final TupleTag<KV<TableReference, TableRow>> RECOVERABLE_ERRORS = new TupleTag<KV<TableReference, TableRow>>() {
    };

    private final String recoverableMissingFieldName;

    public ChannelSchemaErrors(String recoverableMissingFieldName) {
      this.recoverableMissingFieldName = recoverableMissingFieldName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      BigQueryInsertError error = context.element();
      LOG.info("Captured insert error {}",error.getError().toPrettyString());
      error.getError().getErrors().forEach(err -> {
        if (err.getLocation().equals(recoverableMissingFieldName)) {
          context.output(RECOVERABLE_ERRORS, KV.of(error.getTable(), error.getRow()));
        } else {
          context.output(NON_RECOVERABLE_ERRORS, error);
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
