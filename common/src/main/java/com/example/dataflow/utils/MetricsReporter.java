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

/**
 *
 */
public class MetricsReporter implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsReporter.class);

  private final ExecutorService execService = Executors.newSingleThreadExecutor();
  private MetricsReporterCallable reporterCallable;

  private MetricsReporter() {
  }

  public static MetricsReporter create(PipelineResult result) {
    return create(result, null);
  }

  public static MetricsReporter create(PipelineResult result, List<MetricNameFilter> nameFilters) {
    MetricsReporter reporter = new MetricsReporter();
    reporter.reporterCallable = new MetricsReporterCallable(result, nameFilters);
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

    private final PipelineResult result;
    private final Optional<List<MetricNameFilter>> nameFilters;
    private Boolean keepGoing = true;

    public MetricsReporterCallable(PipelineResult result, List<MetricNameFilter> nameFilters) {
      this.result = result;
      this.nameFilters = Optional.ofNullable(nameFilters);
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
