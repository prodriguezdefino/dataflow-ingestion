package com.example.dataflow.transforms;

import com.example.dataflow.utils.Utilities;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class WriteFormatToFileDestinationTest {

  @Rule
  public transient TestPipeline testPipeline = TestPipeline.create();
  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testAvroWrites() throws IOException {

    String outputPath = temporaryFolder.getRoot().getAbsolutePath() + '/';
    List<GenericRecord> records = ComposeFilesTest.generateGenericRecords(2);
    AvroGenericCoder coder = AvroGenericCoder.of(ComposeFilesTest.SCHEMA);
    Instant baseTime = new Instant(0);

    TestStream<GenericRecord> stream
            = TestStream
                    .create(coder)
                    .advanceWatermarkTo(baseTime)
                    .addElements(TimestampedValue.of(records.get(0), Instant.now()))
                    .advanceProcessingTime(Duration.standardSeconds(1L))
                    .addElements(TimestampedValue.of(records.get(1), Instant.now()))
                    .advanceProcessingTime(Duration.standardMinutes(1L))
                    .advanceWatermarkToInfinity();

    testPipeline.getCoderRegistry().registerCoderForType(TypeDescriptor.of(GenericRecord.class), coder);

    // we will write 2 files in the temp directory
    testPipeline
            .apply(stream)
            .apply(Window
                    .<GenericRecord>into(FixedWindows.of(Utilities.parseDuration("1m")))
                    .discardingFiredPanes())
            .apply("WriteParquetToGCS",
                    WriteFormatToFileDestination.<GenericRecord>create()
                            .withSinkProvider(
                                    () -> ParquetIO
                                            .sink(new Schema.Parser().parse(ComposeFilesTest.SCHEMA_STRING))
                                            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED))
                            .withNumShards(1)
                            .withCoder(coder)
                            .withOutputDirectory(
                                    ValueProvider.StaticValueProvider.of(outputPath))
                            .withOutputFilenamePrefix(ValueProvider.StaticValueProvider.of("test"))
                            .withOutputFilenameSuffix(ValueProvider.StaticValueProvider.of(".parquet"))
                            .withTempDirectory(
                                    ValueProvider.StaticValueProvider.of(outputPath + "temp"))
                            .withSuccessFilesWindowDuration("5s")
                            // to avoid generating infinit long sequences for empty windows
                            .withTestingSeq());

    testPipeline.run().waitUntilFinish();

    // create the context object needed for the DoFn test
    List<String> resourceList = Files
            .walk(temporaryFolder.getRoot().toPath())
            .filter(Files::isRegularFile)
            .map(p -> p.toAbsolutePath().toString())
            .collect(Collectors.toList());

    // there has been files written
    Assert.assertTrue(!resourceList.isEmpty());
    // there is a success file
    Assert.assertTrue(resourceList.stream().anyMatch(f -> f.endsWith("SUCCESS")));
    // there are more files with data
    Assert.assertTrue(resourceList.size() > 1);
  }
}
