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
package com.example.dataflow;

import com.google.api.client.util.Joiner;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into windowed Avro files at the specified output
 * directory.
 *
 * <p>
 * Output files will have the following schema:
 *
 * <pre>
 *   {
 *      "type": "record",
 *      "name": "Event"
 *      "namespace": "com.example.dataflow",
 *      "fields": [
 *          {"name": "id", "type": "string"},"
 *          {"name": "isActive", "type": "boolean"},"
 *          {"name": "balance", "type": "double"},"
 *          {"name": "picture", "type": "string"},"
 *          {"name": "age", "type": "long"},"
 *          {"name": "eyeColor", "type": "string"},"
 *          {"name": "name", "type": "string"},"
 *          {"name": "gender", "type": "string"},"
 *          {"name": "company", "type": "string"},"
 *          {"name": "email", "type": "string"},"
 *          {"name": "phone", "type": "string"},"
 *          {"name": "address", "type": "string"},"
 *          {"name": "registered", "type": "long"},"
 *          {"name": "latitude", "type": "double"},"
 *          {"name": "longitude", "type": "double"},"
 *          {"name": "tags", "type": {"type": "array", "items": "string"}},"
 *          {"name": "timestamp", "type": "long"},"
 *          {"name": "about", "type": "string"},"
 *          {"name": "about2", "type": "string"},"
 *          {"name": "about3", "type": "string"},"
 *          {"name": "about4", "type": "string"},"
 *          {"name": "about5", "type": "string"}"
 *      ]
 *   }
 * </pre>
 *
 * <p>
 * Example Usage:
 *
 * <pre>
 * mvn compile exec:java -Dexec.mainClass=com.example.dataflow.PubsubToBigQuery -Dexec.cleanupDaemonThreads=false -Dexec.args=" \
 * --project=$PROJECT \
 * --avroSchemaFileLocation='avro-schema.json' \
 * --stagingLocation=gs://$STAGING_BUCKET/dataflow/staging \
 * --tempLocation=gs://$STAGING_BUCKET/dataflow/temp \
 * --numWorkers=50 \
 * --maxNumWorkers=99 \
 * --runner=DataflowRunner \
 * --workerMachineType=n1-highmem-2 \
 * --usePublicIps=false \
 * --region=us-central1 \
 * --numShards=70 \
 * --inputLocation=gs://$BUCKET/parquet/2021/10/04/17/*.parquet \
 * --outputLocation=gs://$BUCKET/csv/ \
 * --jobName='gcs-avro-to-csv' "
 * </pre>
 */
public class GCSParquetToCSV {

  private static final Logger LOG = LoggerFactory.getLogger(GCSParquetToCSV.class);

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws IOException {

    GCSParquetToCSVOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSParquetToCSVOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  static PipelineResult run(GCSParquetToCSVOptions options) throws IOException {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // read AVRO schema from local filesystem
    final String avroSchemaStr = Files.readAllLines(Paths.get(options.getAvroSchemaFileLocation()))
            .stream()
            .collect(Collectors.joining("\n"));

    LOG.info("Pipeline will be using AVRO schema:\n{}", avroSchemaStr);

    final Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
    final List<String> colNames = avroSchema.getFields().stream().map(f -> f.name()).collect(Collectors.toList());

    pipeline.apply("MatchParquetFiles", FileIO.match().filepattern(options.getInputLocation()))
            .apply("ReadMatches", FileIO.readMatches())
            .apply("ReadGenericRecords", ParquetIO.readFiles(avroSchema))
            .apply("CategorizeOnEyeColor", ParDo.of(new KVsFromEvent()))
            .apply("WriteToCSV",
                    FileIO.<String, KV<String, String>>writeDynamic()
                            // write to this output prefix
                            .to(options.getOutputLocation())
                            // using the key as the destination
                            .by(kv -> kv.getKey())
                            // with our custom sink 
                            .via(new CSVSink(colNames))
                            // with out custom filename pattern, per destination
                            .withNaming(destinationKey -> new DestinationFileNaming(destinationKey))
                            // needed since we are using a custom destination
                            .withDestinationCoder(StringUtf8Coder.of())
                            // force configured number of files per eye color (default 1)
                            .withNumShards(options.getNumShards()));

    // Execute the pipeline and return the result.
    PipelineResult result = pipeline.run();

    // launch the async metrics reporter
    ExecutorService exec = Executors.newSingleThreadExecutor();
    MetricsReporterCallable reporter = new MetricsReporterCallable(result);
    exec.submit(reporter);
    
    // wait for the pipeline to finish
    result.waitUntilFinish();

    // stop the async reporter
    reporter.stopCallable();
    
    // Request the metric called "records_processed" in namespace of our DoFn class "KVsFromEvent"
    MetricQueryResults metrics
            = result
                    .metrics()
                    .queryMetrics(
                            MetricsFilter.builder()
                                    .build());

    LOG.info("***** Printing Final Values for Custom Metrics (All of them) *****");

    // We expect only one result here, and we are retrieving the committed 
    // values since our pipeline already finished (wait until finished)
    for (MetricResult<Long> counter : metrics.getCounters()) {
      LOG.info(counter.getName() + ":" + counter.getCommitted());
      LOG.info(counter.getName() + ":" + counter.getAttempted());
    }

    exec.shutdownNow().stream().forEach(System.out::println);
    
    return result;
  }

  static class MetricsReporterCallable implements Callable<Void> {

    private final PipelineResult result;
    private Boolean keepGoing = true;

    public MetricsReporterCallable(PipelineResult result) {
      this.result = result;
    }

    @Override
    public Void call() throws Exception {

      while (keepGoing) {
        retrieveAndPrintMetrics();
        // wait 10 seconds for next report
        Thread.sleep(10 * 1000);
      }
      return (Void) null;
    }

    public void stopCallable() {
      this.keepGoing = false;
    }

    private void retrieveAndPrintMetrics() {
      // Request the metric called "records_processed" in namespace of our DoFn class "KVsFromEvent"
      MetricQueryResults metrics
              = result
                      .metrics()
                      .queryMetrics(
                              MetricsFilter.builder()
                                      .addNameFilter(
                                              MetricNameFilter.named(KVsFromEvent.class, KVsFromEvent.RECORDS_PROCESSED_COUNTER_NAME))
                                      .build());

      LOG.info("***** Printing Current Custom Metrics values (Specific Metric) *****");

      // We expect only one result here, and we are retrieving the attempted 
      // values since our pipeline is still running
      for (MetricResult<Long> counter : metrics.getCounters()) {
        LOG.info(counter.getName() + ":" + counter.getAttempted());
      }
    }
  }

  static class CSVSink implements FileIO.Sink<KV<String, String>> {

    private final String header;
    private PrintWriter writer;

    public CSVSink(List<String> colNames) {
      this.header = Joiner.on(',').join(colNames);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer = new PrintWriter(Channels.newOutputStream(channel));
      writer.println(header);
    }

    @Override
    public void write(KV<String, String> element) throws IOException {
      writer.println(element.getValue());
    }

    @Override
    public void flush() throws IOException {
      writer.flush();
    }
  }

  static class KVsFromEvent extends DoFn<GenericRecord, KV<String, String>> {

    static final String RECORDS_PROCESSED_COUNTER_NAME = "records_processed";
    private static final Counter COUNTER = Metrics.counter(KVsFromEvent.class, RECORDS_PROCESSED_COUNTER_NAME);

    @ProcessElement
    public void processElement(ProcessContext context) {
      COUNTER.inc();
      GenericRecord gr = context.element();
      // this will only consider first level fields as columns on our CSV 
      String row = gr.getSchema()
              .getFields()
              .stream()
              .map(field -> gr.get(field.name()).toString())
              .map(r -> "\"" + r + "\"")
              .collect(Collectors.joining(","));

      context.output(KV.of(gr.get("eyeColor").toString(), row));
    }
  }

  static class DestinationFileNaming implements FileIO.Write.FileNaming {

    private static final Logger LOG = LoggerFactory.getLogger(DestinationFileNaming.class);

    private final String prefix;

    public DestinationFileNaming(String destinationKey) {
      this.prefix = destinationKey;
    }

    @Override
    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
      StringBuilder fileNameSB = new StringBuilder(prefix);

      if (window != null) {
        if (window instanceof GlobalWindow) {
          fileNameSB.append("-window-").append(
                  DateTime.now().toString(ISODateTimeFormat.basicDateTime()));
        } else {
          fileNameSB.append("-window-").append(
                  window.maxTimestamp().toDateTime().toString(ISODateTimeFormat.basicDateTime()));
        }
      }

      fileNameSB.append("-pane-").append(pane.getIndex())
              .append("-shard-").append(shardIndex).append("-of-").append(numShards)
              .append(".csv");

      if (!compression.equals(Compression.UNCOMPRESSED)) {
        fileNameSB.append(".").append(compression.name().toLowerCase());
      }

      LOG.debug("Windowed file name policy created: {}", fileNameSB.toString());
      return fileNameSB.toString();
    }

  }

}
