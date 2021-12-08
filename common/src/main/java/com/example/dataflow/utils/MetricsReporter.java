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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
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
import java.util.stream.StreamSupport;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

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

    private void retrieveAndPrintMetrics() throws IOException {
      retrieveAndPrintCustomDataflowMetrics();
      retrieveAndPrintPCollectionMetrics();
    }

    private void retrieveAndPrintPCollectionMetrics() throws IOException {
      retrieveAndPrintPCollectionMetrics(metricServiceClient, result, pcolName, REPORT_FREQ_MILLIS);
    }

    @VisibleForTesting
    static void retrieveAndPrintPCollectionMetrics(
            MetricServiceClient metricsServiceClient,
            PipelineResult result,
            Optional<String> pcolName,
            Integer lastFreqReportedMs) throws IOException {

      LOG.info("PCollection Element Counts Metrics");
      retrieveAllFinalPCollectionElementCountMetric(result)
              .forEach(elem
                      -> LOG.info(String.format("%s - value: %d", elem.getKey(), elem.getValue())));
      LOG.info("PCollection Estimated Byte Counts Metrics");
      retrieveAllFinalPCollectionEstimateBytesMetric(result)
              .forEach(elem
                      -> LOG.info(String.format("%s - value: %d", elem.getKey(), elem.getValue())));
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
  
  /**
   * Retrieves the execution time for the current state of the job.
   * @param dataflowService an instance of the dataflow service client.
   * @param projectId the project identifier.
   * @param jobId the job identifier.
   * @return the execution time since start to the current job's state.
   * @throws IOException
   */
  static Long getJobExecutionTimeInMillis(Dataflow dataflowService,
                                          String projectId, String jobId) throws IOException {
    DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTime();

    Dataflow.Projects.Jobs.Get request = dataflowService.projects().jobs().get(projectId, jobId);
    Job job = request.execute();
    DateTime startDateTime = dateFormatter.parseDateTime(job.getStartTime());
    DateTime currentStateDateTime = dateFormatter.parseDateTime(job.getCurrentStateTime());
    return currentStateDateTime.getMillis() - startDateTime.getMillis();
  }

  /**
   * Retrieves the execution time for the current state of the job.
   * @param dataflowService an instance of the dataflow service client.
   * @param result the pipeline result object (must be a DataflowPipelineJob instance).
   * @return the execution time since start to the current job's state.
   * @throws IOException
   */
  static Long getJobExecutionTimeInMillis(Dataflow dataflowService,
                                          PipelineResult result) throws IOException {
    DataflowPipelineJob dfJob = (DataflowPipelineJob) result;
    return getJobExecutionTimeInMillis(dataflowService, dfJob.getProjectId(), dfJob.getJobId());
  }

  // wait 3 mins for next report
  static final int REPORT_FREQ_MILLIS = 3 * 60 * 1000;
  static final String PCOLLECTION_METRIC_ELEMCOUNT = "metric.type=\"dataflow.googleapis.com/job/element_count\" ";
  static final String PCOLLECTION_METRIC_BYTECOUNT = "metric.type=\"dataflow.googleapis.com/job/estimated_byte_count\" ";
  static final String JOB_FILTER_STRING = "resource.type=\"dataflow_job\" metric.label.\"job_id\"=\"%s\" ";
  static final String PCOLLECTION_FILTER_STRING = " metric.label.\"pcollection\"=\"%s\"";
  static final String PCOLLECTION_LABEL = "pcollection";
  static final String METRIC_NA = "NA";

  /**
   * Returns the element count for all the PCollections reported by the job.
   *
   * @param jobInfo The pipeline result object that represents the job.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveAllFinalPCollectionElementCountMetric(
          PipelineResult jobInfo
  ) throws IOException {
    DataflowPipelineJob dfJobInfo = (DataflowPipelineJob) jobInfo;
    return retrieveAllFinalPCollectionElementCountMetric(dfJobInfo.getProjectId(), dfJobInfo.getJobId());
  }

  /**
   * Returns the element count for all the PCollections reported by the job.
   *
   * @param jobInfo The pipeline result object that represents the job.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveAllFinalPCollectionElementCountMetric(
          String projectId,
          String jobId
  ) throws IOException {
    String filter = String.format(JOB_FILTER_STRING, jobId)
            + PCOLLECTION_METRIC_ELEMCOUNT;
    return retrievePCollectionMetrics(
            projectId, MetricServiceClient.create(), filter, REPORT_FREQ_MILLIS);
  }

  /**
   * Returns the output byte counts for all the PCollections reported by the job. Since this metrics is reported less frequently data may
   * take longer to appear as a metric point.
   *
   * @param jobInfo The pipeline result object that represents the job.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveAllFinalPCollectionEstimateBytesMetric(
          PipelineResult jobInfo
  ) throws IOException {
    DataflowPipelineJob dfJobInfo = (DataflowPipelineJob) jobInfo;
    return retrieveAllFinalPCollectionEstimateBytesMetric(dfJobInfo.getProjectId(), dfJobInfo.getJobId());
  }

  /**
   * Returns the output byte counts for all the PCollections reported by the job. Since this metrics is reported less frequently data may
   * take longer to appear as a metric point.
   *
   * @param projectId GCP project id.
   * @param jobId Dataflow job id.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveAllFinalPCollectionEstimateBytesMetric(
          String projectId,
          String jobId
  ) throws IOException {
    String filter = String.format(JOB_FILTER_STRING, jobId)
            + PCOLLECTION_METRIC_BYTECOUNT;
    return retrievePCollectionMetrics(
            projectId, MetricServiceClient.create(), filter, REPORT_FREQ_MILLIS);
  }

  /**
   * Returns the output byte count for the specified PCollection (if exists) reported by the job.
   *
   * @param jobInfo The pipeline result object that represents the job.
   * @param pCollectionName The PCollection's name.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveFinalPCollectionEstimateBytesMetric(
          PipelineResult jobInfo,
          String pCollectionName
  ) throws IOException {
    DataflowPipelineJob dfJobInfo = (DataflowPipelineJob) jobInfo;
    return retrieveFinalPCollectionEstimateBytesMetric(dfJobInfo.getProjectId(), dfJobInfo.getJobId(), pCollectionName);
  }

  /**
   * Returns the output byte count for the specified PCollection (if exists) reported by the job.
   *
   * @param projectId GCP project id.
   * @param jobId Dataflow job id.
   * @param pCollectionName The PCollection's name.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveFinalPCollectionEstimateBytesMetric(
          String projectId,
          String jobId,
          String pCollectionName
  ) throws IOException {
    String filter = String.format(JOB_FILTER_STRING, jobId)
            + PCOLLECTION_METRIC_BYTECOUNT
            + String.format(PCOLLECTION_FILTER_STRING, pCollectionName);
    return retrievePCollectionMetrics(
            projectId, MetricServiceClient.create(), filter, REPORT_FREQ_MILLIS);
  }

  /**
   * Returns the element count for the specified PCollection (if exists) reported by the job.
   *
   * @param jobInfo The pipeline result object that represents the job.
   * @param pCollectionName The PCollection's name.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveFinalPCollectionElementCountMetric(
          PipelineResult jobInfo,
          String pCollectionName
  ) throws IOException {
    DataflowPipelineJob dfJobInfo = (DataflowPipelineJob) jobInfo;
    return retrieveFinalPCollectionElementCountMetric(dfJobInfo.getProjectId(), dfJobInfo.getJobId(), pCollectionName);
  }

  /**
   * Returns the element count for the specified PCollection (if exists) reported by the job.
   *
   * @param projectId GCP project id.
   * @param jobId Dataflow job id.
   * @param pCollectionName The PCollection's name.
   * @return A list of labels and values for the reported metrics.
   * @throws IOException In case the creation of the metrics client fails.
   */
  static List<KV<String, Long>> retrieveFinalPCollectionElementCountMetric(
          String projectId,
          String jobId,
          String pCollectionName
  ) throws IOException {
    String filter = String.format(JOB_FILTER_STRING, jobId)
            + PCOLLECTION_METRIC_ELEMCOUNT
            + String.format(PCOLLECTION_FILTER_STRING, pCollectionName);
    return retrievePCollectionMetrics(
            projectId, MetricServiceClient.create(), filter, REPORT_FREQ_MILLIS);
  }

  /**
   * Retrieves reported PCollection metrics for the job.
   *
   * @param jobInfo The pipeline result object for the job
   * @param metricServiceClient an instance of the metric service client
   * @param filter a string filter for the metric service query
   * @param lastFreqReportedMs the time in ms to consider for the request
   * @return a list of KV containing the label of the metric (PCollection name) and the value of the returned metric (based on the filter).
   */
  static List<KV<String, Long>> retrievePCollectionMetrics(
          String projectId,
          MetricServiceClient metricServiceClient,
          String filter,
          Integer lastFreqReportedMs) {

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
                    .build();

    // Send the request to list the time series
    MetricServiceClient.ListTimeSeriesPagedResponse response
            = metricServiceClient.listTimeSeries(request);

    // Process the response
    return StreamSupport
            .stream(response.iterateAll().spliterator(), false)
            .map(
                    timeSeries -> {
                      Long value = timeSeries
                              .getPointsList()
                              .stream()
                              .min((Point o1, Point o2)
                                      -> Long.compare(
                                      o2.getInterval().getEndTime().getSeconds(),
                                      o1.getInterval().getEndTime().getSeconds()))
                              .map(point -> point.getValue().getDoubleValue())
                              .map(Double::longValue)
                              .orElse(Long.MIN_VALUE);

                      String labels = timeSeries
                              .getMetric()
                              .getLabelsMap()
                              .entrySet()
                              .stream()
                              .filter(entry -> PCOLLECTION_LABEL.equals(entry.getKey()))
                              .map(entry
                                      -> String.format("\tLabel: %s - %s", entry.getKey(), entry.getValue()))
                              .findFirst()
                              .orElse(METRIC_NA);

                      return KV.of(labels, value);
                    })
            .collect(Collectors.toList());
  }
  
}
