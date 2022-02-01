package com.example.dataflow.utils;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

/**
 *
 */
public class AvroJsonLogicalType extends LogicalType {

  //The key to use as a reference to the type
  public static final String JSON_LOGICAL_TYPE_NAME = "json";

  public static AvroJsonLogicalType get() {
    return new AvroJsonLogicalType();
  }

  AvroJsonLogicalType() {
    super(JSON_LOGICAL_TYPE_NAME);
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    if (schema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException(
              "Logical type 'json' must be backed by string");
    }
  }

  public static class JsonAvroConversion extends Conversion<String> {

    private static final JsonAvroConversion INSTANCE = new JsonAvroConversion();

    public static final JsonAvroConversion get() {
      return INSTANCE;
    }

    private JsonAvroConversion() {
      super();
    }

    @Override
    public Class<String> getConvertedType() {
      return String.class;
    }

    @Override
    public String getLogicalTypeName() {
      return JSON_LOGICAL_TYPE_NAME;
    }

    @Override
    public CharSequence toCharSequence(String value, Schema schema, LogicalType type) {
      return value;
    }

    @Override
    public String fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return value.toString();
    }
  }
}
