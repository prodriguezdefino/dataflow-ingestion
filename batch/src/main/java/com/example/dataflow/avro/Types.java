/*
 * Copyright (C) 2023 Google Inc.
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
package com.example.dataflow.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** Collection of types and their coders used by the pipeline execution. */
public interface Types {

  record FilenameAndSchema(String fileName, String avroSchema) {}

  static class FilenameAndSchemaCoder extends CustomCoder<FilenameAndSchema> {

    static final FilenameAndSchemaCoder INSTANCE = new FilenameAndSchemaCoder();
    static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

    private FilenameAndSchemaCoder() {}

    public static FilenameAndSchemaCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(FilenameAndSchema value, OutputStream outStream)
        throws CoderException, IOException {
      stringCoder.encode(value.fileName(), outStream);
      stringCoder.encode(value.avroSchema(), outStream);
    }

    @Override
    public FilenameAndSchema decode(InputStream inStream) throws CoderException, IOException {
      return new FilenameAndSchema(stringCoder.decode(inStream), stringCoder.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  record ElementWithSchema(String avroSchema, byte[] encodedGenericRecord) {}

  static class ElementWithSchemaCoder extends CustomCoder<ElementWithSchema> {

    static final ElementWithSchemaCoder INSTANCE = new ElementWithSchemaCoder();
    static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    static final ByteArrayCoder binaryCoder = ByteArrayCoder.of();

    private ElementWithSchemaCoder() {}

    public static ElementWithSchemaCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ElementWithSchema value, OutputStream outStream)
        throws CoderException, IOException {
      stringCoder.encode(value.avroSchema(), outStream);
      binaryCoder.encode(value.encodedGenericRecord(), outStream);
    }

    @Override
    public ElementWithSchema decode(InputStream inStream) throws CoderException, IOException {
      return new ElementWithSchema(stringCoder.decode(inStream), binaryCoder.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  record ElementWithSchemaAndFilename(
      String fileName, String avroSchema, byte[] encodedGenericRecord) {}

  static class ElementWithSchemaAndFilenameCoder extends CustomCoder<ElementWithSchemaAndFilename> {

    static final ElementWithSchemaAndFilenameCoder INSTANCE =
        new ElementWithSchemaAndFilenameCoder();
    static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    static final ByteArrayCoder binaryCoder = ByteArrayCoder.of();

    private ElementWithSchemaAndFilenameCoder() {}

    public static ElementWithSchemaAndFilenameCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ElementWithSchemaAndFilename value, OutputStream outStream)
        throws CoderException, IOException {
      stringCoder.encode(value.fileName(), outStream);
      stringCoder.encode(value.avroSchema(), outStream);
      binaryCoder.encode(value.encodedGenericRecord(), outStream);
    }

    @Override
    public ElementWithSchemaAndFilename decode(InputStream inStream)
        throws CoderException, IOException {
      return new ElementWithSchemaAndFilename(
          stringCoder.decode(inStream), stringCoder.decode(inStream), binaryCoder.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
