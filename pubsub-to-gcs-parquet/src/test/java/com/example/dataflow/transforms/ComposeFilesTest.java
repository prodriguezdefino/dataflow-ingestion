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

import com.example.dataflow.PubsubToGCSParquet;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class ComposeFilesTest {

  public static final String SCHEMA_STRING
          = "{"
          + "\"type\":\"record\", "
          + "\"name\":\"testrecord\","
          + "\"fields\":["
          + "    {\"name\":\"name\",\"type\":\"string\"},"
          + "    {\"name\":\"id\",\"type\":\"string\"}"
          + "  ]"
          + "}";
  public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
  public static final String[] SCIENTISTS
          = new String[]{
            "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
            "Faraday", "Newton", "Bohr", "Galilei", "Maxwell"
          };

  @Rule
  public transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule
  public transient TestPipeline testPipeline = TestPipeline.create();
  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  public ComposeFilesTest() {
  }

  public static List<GenericRecord> generateGenericRecords(long count) {
    List<GenericRecord> data = new ArrayList<>();
    GenericRecordBuilder builder = new GenericRecordBuilder(SCHEMA);
    for (int i = 0; i < count; i++) {
      int index = i % SCIENTISTS.length;
      GenericRecord record
              = builder.set("name", SCIENTISTS[index]).set("id", Integer.toString(i)).build();
      data.add(record);
    }
    return data;
  }

  @Test
  public void testComposeParquetFiles() {
    List<GenericRecord> records = generateGenericRecords(1000);

    // we will write 2 files in the temp directory
    mainPipeline
            .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                    FileIO.<GenericRecord>write()
                            .via(ParquetIO
                                    .sink(SCHEMA)
                                    .withCompressionCodec(CompressionCodecName.SNAPPY))
                            .to(temporaryFolder.getRoot().getAbsolutePath())
                            .withNumShards(2));

    mainPipeline.run().waitUntilFinish();

    // create the context object needed for the DoFn test
    List<String> resourceList = Arrays.asList(temporaryFolder.getRoot().list())
            .stream()
            .map(fileStr -> temporaryFolder.getRoot().getAbsolutePath() + "/" + fileStr)
            .filter(absFileStr -> Paths.get(absFileStr).toFile().isFile())
            .collect(Collectors.toList());

    String destinationPath = temporaryFolder.getRoot().getAbsolutePath() + "/compose-output.parquet";

    ComposeFiles.ExecComposeFiles<GenericRecord> cfiles
            = new ComposeFiles.ExecComposeFiles<GenericRecord>()
                    .withSinkProvider(
                            () -> ParquetIO
                                    .sink(SCHEMA)
                                    .withCompressionCodec(CompressionCodecName.SNAPPY))
                    .withComposeFunction(PubsubToGCSParquet::composeParquetFiles);

    // register coder for the test pipeline
    testPipeline.getCoderRegistry().registerCoderForClass(
            ComposeFiles.ComposeContext.class,
            ComposeFiles.ComposeContextCoder.of());

    PCollection<ComposeFiles.ComposeContext> ctxPC
            = testPipeline
                    .apply(Create.of(resourceList))
                    // first match all the files to be processed
                    .apply(FileIO.matchAll())
                    // capture readable matches
                    .apply(FileIO.readMatches())
                    // create the compose context object
                    .apply(WithKeys.of((Void) null))
                    .apply(GroupByKey.create())
                    .apply(MapElements
                            .into(TypeDescriptor.of(ComposeFiles.ComposeContext.class))
                            .via(readableFiles
                                    -> ComposeFiles.ComposeContext.of(
                                    1,
                                    1,
                                    null,
                                    destinationPath,
                                    StreamSupport
                                            .stream(readableFiles.getValue().spliterator(), false)
                                            .collect(Collectors.toList()))))
                    // compose the files
                    .apply(ParDo.of(cfiles));

    PAssert.that(ctxPC).satisfies(elem -> {
      Assert.assertNotNull(elem);
      List<ComposeFiles.ComposeContext> composeCtxs
              = StreamSupport.stream(elem.spliterator(), false).collect(Collectors.toList());
      Assert.assertEquals(1, composeCtxs.size());
      ComposeFiles.ComposeContext composeCtx = composeCtxs.get(0);
      File output = Paths.get(destinationPath).toFile();
      Assert.assertEquals(output.isFile(), true);
      Assert.assertTrue("Output file should not be empty.", 0 <= output.length());
      return null;
    });

    testPipeline.run().waitUntilFinish();
  }

}
