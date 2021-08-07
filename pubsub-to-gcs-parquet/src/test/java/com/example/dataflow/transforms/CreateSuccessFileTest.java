package com.example.dataflow.transforms;

import com.example.dataflow.utils.Utilities;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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
public class CreateSuccessFileTest {

  @Rule
  public transient TestPipeline testPipeline = TestPipeline.create();
  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSuccessFileOnDataWindow() throws IOException {

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

    TupleTag<Boolean> dataOnWindowSignalsTag = CreateSuccessFiles.dataOnWindowSignalTag();
    TupleTag<GenericRecord> dataToBeIngestedTag = WriteFormatToGCS.CaptureDataOnWindowSignals
            .<GenericRecord>createDataToBeProcessedTag();

    testPipeline.getCoderRegistry().registerCoderForType(TypeDescriptor.of(GenericRecord.class), coder);

    // we will write 2 files in the temp directory
    PCollectionTuple capturedData = testPipeline
            .apply(stream)
            .apply(Window
                    .<GenericRecord>into(FixedWindows.of(Utilities.parseDuration("1m")))
                    .discardingFiredPanes())
            .apply(ParDo
                    .of(
                            new WriteFormatToGCS.CaptureDataOnWindowSignals<>(
                                    dataOnWindowSignalsTag,
                                    dataToBeIngestedTag))
                    .withOutputTags(dataToBeIngestedTag, TupleTagList.of(dataOnWindowSignalsTag)));

    PCollection<String> fileNames = capturedData.get(dataToBeIngestedTag)
            .setCoder(coder)
            .apply(
                    FileIO.<GenericRecord>write()
                            .via(ParquetIO
                                    .sink(ComposeFilesTest.SCHEMA)
                                    .withCompressionCodec(CompressionCodecName.SNAPPY))
                            .to(outputPath)
                            .withNumShards(2))
            .getPerDestinationOutputFilenames()
            .apply(Values.create());

    TupleTag<String> fileNamesTag = CreateSuccessFiles.processedDataTag();

    PCollectionTuple.of(dataOnWindowSignalsTag, capturedData.get(dataOnWindowSignalsTag))
            .and(fileNamesTag, fileNames)
            .apply(CreateSuccessFiles
                    .create()
                    .withDataOnWindowSignalsTag(dataOnWindowSignalsTag)
                    .withProcessedDataTag(fileNamesTag)
                    .withSuccessFileWindowDuration("1m")
                    .withOutputDirectory(ValueProvider.StaticValueProvider.of(outputPath))
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
