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
package com.example.dataflow.utils;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.Duration;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.util.Timestamps;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;

/**
 *
 */
public class MetricsReporter implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsReporter.class);

  private final ExecutorService execService = Executors.newSingleThreadExecutor();
  private MetricsReporterCallable reporterCallable;

  private MetricsReporter() {
  }

  /**
   * Creates an auto-closeable metrics reporter, which will run on a separated thread to capture and print Dataflow job's metrics (custom or
   * PCollection related).
   *
   * @param result a job result object reference, to correctly capture pcollection related metrics this should be an instance of a Dataflow
   * job result object
   * @param nameFilters a List of metric name filters, if null is provided all the existing custom metrics will be printed
   * @param pcolName a PCollection name to filter, if null is provided all the existing PCollection metrics will be printed
   * @return a metrics reporter object
   * @throws IOException
   */
  public static MetricsReporter create(
          PipelineResult result, List<MetricNameFilter> nameFilters, String pcolName) throws IOException {
    MetricsReporter reporter = new MetricsReporter();
    reporter.reporterCallable = new MetricsReporterCallable(result, nameFilters, pcolName);
    reporter.execService.submit(reporter.reporterCallable);
    return reporter;
  }

  public void stopReporter() {
    reporterCallable.stopCallable();
  }

  @Override
  public void close() throws Exception {
    reporterCallable.stopCallable();
    execService.shutdownNow();
  }

  static class MetricsReporterCallable implements Callable<Void> {

    // wait 3 mins for next report
    private static final int REPORT_FREQ_MILLIS = 3 * 60 * 1000;
    private static final String PCOLLECTION_METRIC_ELEMCOUNT = "metric.type=\"dataflow.googleapis.com/job/element_count\" ";
    private static final String PCOLLECTION_METRIC_BYTECOUNT = "metric.type=\"dataflow.googleapis.com/job/estimated_byte_count\" ";
    private static final String JOB_FILTER_STRING = "resource.type=\"dataflow_job\" metric.label.\"job_id\"=\"%s\"";
    private static final String PCOLLECTION_FILTER_STRING = " metric.label.\"pcollection\"=\"%s\"";

    private final DataflowPipelineJob result;
    private final Optional<List<MetricNameFilter>> nameFilters;
    private final Optional<String> pcolName;
    private final String jobId;
    private final String projectId;
    private Boolean keepGoing = true;
    MetricServiceClient metricServiceClient;

    public MetricsReporterCallable(
            PipelineResult result,
            List<MetricNameFilter> nameFilters,
            String pcolName) throws IOException {
      this.result = (DataflowPipelineJob) result;
      this.nameFilters = Optional.ofNullable(nameFilters);
      this.pcolName = Optional.ofNullable(pcolName);
      this.jobId = this.result.getJobId();
      this.projectId = this.result.getProjectId();
      this.metricServiceClient = MetricServiceClient.create();
    }

    @Override
    public Void call() throws Exception {

      while (keepGoing) {
        retrieveAndPrintMetrics();
        Thread.sleep(REPORT_FREQ_MILLIS);
      }
      return (Void) null;
    }

    public void stopCallable() {
      this.keepGoing = false;
      this.metricServiceClient.close();
    }

    private void retrieveAndPrintMetrics() {
      retrieveAndPrintCustomDataflowMetrics();
      retrieveAndPrintPCollectionMetrics();
    }

    private void retrieveAndPrintPCollectionMetrics() {
      retrieveAndPrintPCollectionMetrics(metricServiceClient, projectId, jobId, pcolName, REPORT_FREQ_MILLIS);
    }

    @VisibleForTesting
    static void retrieveAndPrintPCollectionMetrics(
            MetricServiceClient metricsServiceClient,
            String projectId,
            String jobId,
            Optional<String> pcolName,
            Integer lastFreqReportedMs) {

      String pColFilter = pcolName
              .map(pcName -> String.format(PCOLLECTION_FILTER_STRING, pcName))
              .orElse("");

      String elemCountFilter = String.format(PCOLLECTION_METRIC_ELEMCOUNT + JOB_FILTER_STRING + pColFilter, jobId);
      retrieveAndPrintPCollectionMetrics(
              metricsServiceClient,
              "PCollection Element Count",
              projectId,
              elemCountFilter,
              lastFreqReportedMs);

      String byteCountFilter = String.format(PCOLLECTION_METRIC_BYTECOUNT + JOB_FILTER_STRING + pColFilter, jobId);
      retrieveAndPrintPCollectionMetrics(
              metricsServiceClient,
              "PCollection Estimated Byte Count",
              projectId,
              byteCountFilter,
              lastFreqReportedMs);
    }

    static void retrieveAndPrintPCollectionMetrics(
            MetricServiceClient metricsServiceClient,
            String metricName,
            String projectId,
            String filter,
            Integer lastFreqReportedMs) {
      try {

        ProjectName projectName = ProjectName.of(projectId);
        long startMillis = System.currentTimeMillis() - lastFreqReportedMs;
        TimeInterval interval
                = TimeInterval.newBuilder()
                        .setStartTime(Timestamps.fromMillis(startMillis))
                        .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
                        .build();

        // Prepares the list time series request
        ListTimeSeriesRequest request
                = ListTimeSeriesRequest.newBuilder()
                        .setName(projectName.toString())
                        .setFilter(filter)
                        .setInterval(interval)
                        .setAggregation(
                                Aggregation.newBuilder()
                                        .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
                                        .setPerSeriesAligner(Aggregation.Aligner.ALIGN_MEAN)
                                        .setCrossSeriesReducer(Aggregation.Reducer.REDUCE_NONE)
                                        .build())
                        .setSecondaryAggregation(
                                Aggregation.newBuilder()
                                        .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
                                        .setCrossSeriesReducer(Aggregation.Reducer.REDUCE_NONE)
                                        .build())
                        .build();

        // Send the request to list the time series
        MetricServiceClient.ListTimeSeriesPagedResponse response
                = metricsServiceClient.listTimeSeries(request);

        // Process the response
        LOG.info("\n************* {} *************: ", metricName);
        response
                .iterateAll()
                .forEach(
                        timeSeries -> {
                          List<Point> points = timeSeries
                                  .getPointsList()
                                  .stream()
                                  .sorted((Point o1, Point o2) -> {
                                    if (o1.getInterval().getEndTime().getSeconds() == o2.getInterval().getEndTime().getSeconds()) {
                                      return 0;
                                    } else if (o1.getInterval().getEndTime().getSeconds() < o2.getInterval().getEndTime().getSeconds()) {
                                      return 1;
                                    } else {
                                      return -1;
                                    }
                                  }).collect(Collectors.toList());

                          String labels = timeSeries
                                  .getMetric()
                                  .getLabelsMap()
                                  .entrySet()
                                  .stream()
                                  .map(entry -> String.format("\tLabel: %s, value: %s", entry.getKey(), entry.getValue()))
                                  .collect(Collectors.joining("\n"));

                          String value = String.format("Value: %d",
                                  points
                                          .stream()
                                          .findFirst()
                                          .map(point -> point.getValue().getDoubleValue())
                                          .map(d -> d.longValue())
                                          .orElse(Long.MIN_VALUE));
                          LOG.info("Filter:\n{}\n{}", labels, value);
                          //OG.info("All data " + timeSeries.toString());
                        });
      } catch (Exception e) {
        LOG.error("Error collecting metrics", e);
      }
    }

    private void retrieveAndPrintCustomDataflowMetrics() {
      MetricsFilter.Builder builder = MetricsFilter.builder();

      // in case we got a list of filters
      nameFilters.ifPresent(filters -> filters.forEach(filter -> builder.addNameFilter(filter)));

      // Request the metrics filtered by the provided list (potentially none)
      MetricQueryResults metrics
              = result
                      .metrics()
                      .queryMetrics(builder.build());

      LOG.info("***** Printing Current Custom Metrics values (Specific Metric) *****");

      // We expect only one result here, and we are retrieving the attempted 
      // values since our pipeline is still running
      for (MetricResult<Long> counter : metrics.getCounters()) {
        LOG.info(counter.getName() + ":" + counter.getCommitted());
      }
    }
  }
}
