/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.dataflow;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Rule;
import org.junit.Test;

/**
 *
 * @author rpablo
 */
public class PubsubToGCSParquetTest {

  PubsubToGCSParquet.PStoGCSParquetOptions options;

  {
    options = PipelineOptionsFactory.as(PubsubToGCSParquet.PStoGCSParquetOptions.class);
    options.setTempLocation("gs://discord-load-test-pabs/test-compose/");
  }

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.fromOptions(options);

  public PubsubToGCSParquetTest() {
  }

  @Test
  public void testSomeMethod() {

    //Storage storage = StorageOptions.getDefaultInstance().getService();
    List<String> resourceList = Arrays.asList(
            "gs://discord-load-test-pabs/test-compose/file1.parquet",
            "gs://discord-load-test-pabs/test-compose/file2.parquet");

    String destinationPath = "gs://discord-load-test-pabs/test-compose/compose-output.parquet";

    PubsubToGCSParquet.ComposeGCSFiles.ComposeContext context = PubsubToGCSParquet.ComposeGCSFiles.ComposeContext.of(1, 2, null, destinationPath, resourceList);

    PubsubToGCSParquet.ComposeGCSFiles.ComposeFiles cfiles = new PubsubToGCSParquet.ComposeGCSFiles.ComposeFiles().withSinkProvider(voidValue -> ParquetIO
            .sink(PubsubToGCSParquet.AVRO_SCHEMA)
            .withCompressionCodec(CompressionCodecName.SNAPPY));

    PCollection<PubsubToGCSParquet.ComposeGCSFiles.ComposeContext> ctxPC
            = pipeline.apply(Create.of(context))
                    .apply(ParDo.of(cfiles));

    pipeline.run();

    PAssert.that(ctxPC).satisfies(elem -> {

      return null;
    });

  }

}
