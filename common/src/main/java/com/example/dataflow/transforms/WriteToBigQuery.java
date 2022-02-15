package com.example.dataflow.transforms;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 *
 */
public abstract class WriteToBigQuery<T, K> extends PTransform<PCollection<T>, PDone> {

  protected final ValueProvider<String> destinationTableSpec;
  protected final String tableJsonSchema;
  protected String partitionColumn = null;

  WriteToBigQuery(ValueProvider<String> tableSpec, TableSchema tableSchema) {
    this.destinationTableSpec = tableSpec;
    this.tableJsonSchema = BigQueryHelpers.toJsonString(tableSchema);
  }

  public WriteToBigQuery<T, K> withDailyPartitionColumn(String columnName) {
    this.partitionColumn = columnName;
    return this;
  }

  public static FileLoadWrite useFileLoads(
          ValueProvider<String> destinationTableSpec,
          TableSchema tableSchema,
          String avroJsonSchemaAsString) {
    return new FileLoadWrite(destinationTableSpec, tableSchema, avroJsonSchemaAsString);
  }

  public static StorageWriteAPIWrite useStorageWrites(
          ValueProvider<String> destinationTableSpec,
          TableSchema tableSchema,
          SerializableFunction<GenericRecord, Row> rowWithJsonEventMapper,
          org.apache.beam.sdk.schemas.Schema rowSchemaWithJsonEvent) {
    return new StorageWriteAPIWrite(destinationTableSpec, tableSchema, rowWithJsonEventMapper, rowSchemaWithJsonEvent);
  }

  protected BigQueryIO.Write<K> baseWritePTransform(BigQueryIO.Write<K> write) {
    var currentWrite = write
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchemaUpdateOptions(
                    Sets.newHashSet(
                            BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                            BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .to(this.destinationTableSpec)
            .withJsonSchema(tableJsonSchema);

    if (partitionColumn != null) {
      currentWrite
              = currentWrite
                      .withTimePartitioning(
                              new TimePartitioning()
                                      .setField(partitionColumn)
                                      .setType("DAY"));
    }

    return currentWrite;
  }

  protected abstract void processWrite(PCollection<T> input);

  @Override
  public PDone expand(PCollection<T> input) {
    processWrite(input);
    return PDone.in(input.getPipeline());
  }

  public static class FileLoadWrite extends WriteToBigQuery<GenericRecord, GenericRecord> {

    private final String avroSchemaAsJsonStr;
    private SerializableFunction<AvroWriteRequest<GenericRecord>, GenericRecord> avroMappingFunc = null;

    FileLoadWrite(ValueProvider<String> tableSpec, TableSchema tableSchema, String avroSchemaAsJsonStr) {
      super(tableSpec, tableSchema);
      this.avroSchemaAsJsonStr = avroSchemaAsJsonStr;
    }

    public FileLoadWrite withAvroMappingFunction(
            SerializableFunction<AvroWriteRequest<GenericRecord>, GenericRecord> avroMappingFunc) {
      this.avroMappingFunc = avroMappingFunc;
      return this;
    }

    @Override
    public void processWrite(PCollection<GenericRecord> input) {
      if (avroMappingFunc == null) {
        avroMappingFunc = avroWriteRequest -> avroWriteRequest.getElement();
      }
      input.apply("WriteToBQWithFileLoads",
              baseWritePTransform(BigQueryIO.<GenericRecord>write())
                      .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                      .withAvroSchemaFactory(tableSchema -> new Schema.Parser().parse(avroSchemaAsJsonStr))
                      // simple writer configuration, not much to do
                      .withAvroWriter(
                              avroMappingFunc,
                              schema -> new GenericDatumWriter<>(schema))
                      .to(this.destinationTableSpec));
    }

  }

  public static class StorageWriteAPIWrite extends WriteToBigQuery<GenericRecord, Row> {

    private final SerializableFunction<GenericRecord, Row> toRowMapper;
    private final org.apache.beam.sdk.schemas.Schema rowSchema;

    StorageWriteAPIWrite(
            ValueProvider<String> tableSpec,
            TableSchema tableSchema,
            SerializableFunction<GenericRecord, Row> toRowMapper,
            org.apache.beam.sdk.schemas.Schema rowSchema) {
      super(tableSpec, tableSchema);
      this.toRowMapper = toRowMapper;
      this.rowSchema = rowSchema;
    }

    @Override
    public void processWrite(PCollection<GenericRecord> input) {
      input
              .apply("TransformToRows",
                      MapElements
                              .into(TypeDescriptors.rows())
                              .via(toRowMapper))
              .setCoder(RowCoder.of(rowSchema))
              .apply("WriteToBQWithStorageWriteAPI",
                      baseWritePTransform(BigQueryIO.<Row>write())
                              .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                              .useBeamSchema());
    }
  }
}
