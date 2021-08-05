package com.example.dataflow.transforms;

import com.example.dataflow.utils.Utilities;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class WriteFormatToGCSTest {

  @Rule
  public transient TestPipeline testPipeline = TestPipeline.create();
  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testAvroWrites() throws IOException {
    List<GenericRecord> records = ComposeFilesTest.generateGenericRecords(1000);
    AvroGenericCoder coder = AvroGenericCoder.of(ComposeFilesTest.SCHEMA);

    // we will write 2 files in the temp directory
    testPipeline
            .apply(Create.of(records).withCoder(coder))
            .apply(Window
                    .<GenericRecord>into(FixedWindows.of(Utilities.parseDuration("5s")))
                    .discardingFiredPanes())
            .apply("WriteParquetToGCS",
                    WriteFormatToGCS.<GenericRecord>create()
                            .withSinkProvider(
                                    () -> ParquetIO
                                            .sink(new Schema.Parser().parse(ComposeFilesTest.SCHEMA_STRING))
                                            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED))
                            .withNumShards(1)
                            .withCreateSuccessFile(false)
                            .withOutputDirectory(
                                    ValueProvider.StaticValueProvider.of(temporaryFolder.getRoot().getAbsolutePath() + "/output"))
                            .withOutputFilenamePrefix(ValueProvider.StaticValueProvider.of("test"))
                            .withOutputFilenameSuffix(ValueProvider.StaticValueProvider.of(".parquet"))
                            .withTempDirectory(
                                    ValueProvider.StaticValueProvider.of(temporaryFolder.getRoot().getAbsolutePath() + "/temp"))
                            .withSuccessFilesWindowDuration("5s"));

    testPipeline.run().waitUntilFinish();

    // create the context object needed for the DoFn test
    List<String> resourceList = Files
            .walk(temporaryFolder.getRoot().toPath().resolve("output"))
            .filter(Files::isRegularFile)
            .map(p -> p.toAbsolutePath().toString())
            .collect(Collectors.toList());

    Assert.assertTrue(!resourceList.isEmpty());

  }
}
