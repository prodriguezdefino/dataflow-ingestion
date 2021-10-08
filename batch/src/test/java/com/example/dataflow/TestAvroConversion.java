package com.example.dataflow;

import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.joda.time.Instant;
import org.junit.Test;

/**
 *
 */
public class TestAvroConversion {

  @Test
  @SuppressWarnings("deprecation")
  public void copyAddField() throws IOException {
    Schema writerSchema = Schema.createRecord("test", null, "avro.test", false);
    writerSchema.setFields(Lists.newArrayList(
            new Field("project", Schema.create(Type.STRING), null, null),
            new Field("city", Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL),
                    Schema.create(Type.STRING))), null, JsonProperties.NULL_VALUE)));
    GenericData.Record record = new GenericRecordBuilder(writerSchema)
            .set("project", "ff").build();
    GenericRecord result = addTimestampField(record);
    System.out.println(result);
  }

  private GenericRecord addTimestampField(GenericRecord input) throws IOException {
    Schema inputSchema = input.getSchema();
    GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<>(inputSchema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

    w.write(input, encoder);
    encoder.flush();

    Schema readerSchema = addNullableTimestampFieldToAvroSchema(inputSchema, "timestamp");

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(inputSchema, readerSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    result.put("timestamp", Instant.now().getMillis());
    return result;
  }

  private Schema addNullableTimestampFieldToAvroSchema(Schema base, String fieldName) {
    Schema timestampMilliType
            = Schema.createUnion(
                    Lists.newArrayList(
                            Schema.create(Type.NULL),
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

}
