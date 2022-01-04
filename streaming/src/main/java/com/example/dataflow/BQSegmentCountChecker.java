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

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatterBuilder;

public class BQSegmentCountChecker {

  static public void main(String[] args) throws InterruptedException {
    Supplier<Stream<String>> argsStream = () -> Arrays.asList(args).stream();
    var tableSpec = argsStream.get()
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Expecting a BQ table spec project.dataset.table"));
    var segmentStartStr = argsStream.get()
            .skip(1)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Expecting a quoted datetime, '2021-12-07 22:00:00'"));
    var minCheckPeriodMs = argsStream.get()
            .skip(2)
            .findFirst()
            .map(Integer::parseInt)
            .orElse(1 * 1000);

    var dateFormatter = new DateTimeFormatterBuilder()
            .appendYear(4, 4)
            .appendLiteral("-")
            .appendMonthOfYear(2)
            .appendLiteral("-")
            .appendDayOfMonth(2)
            .appendLiteral(" ")
            .appendHourOfDay(2)
            .appendLiteral(":")
            .appendMinuteOfHour(2)
            .appendLiteral(":")
            .appendSecondOfMinute(2)
            .toFormatter()
            .withZoneUTC();

    var startDateTime = dateFormatter.parseDateTime(segmentStartStr);
    var endDateTime = startDateTime.plusMinutes(5);
    var segmentEndStr = dateFormatter.print(endDateTime);

    var sqlQuery = String
            .format("SELECT COUNT(*) FROM `%s` WHERE insertionTimestamp BETWEEN '%s' AND '%s'",
                    tableSpec,
                    segmentStartStr,
                    segmentEndStr);

    var bigquery = BigQueryOptions.getDefaultInstance().getService();

    var queryConfig
            = QueryJobConfiguration
                    .newBuilder(sqlQuery)
                    .setUseLegacySql(false)
                    .build();

    System.out.printf("Starting the count check for segment ['%s' - '%s']\n", segmentStartStr, segmentEndStr);

    while (true) {
      var queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());

      queryJob.getQueryResults()
              .iterateAll()
              .forEach(r -> {
                var now = DateTime.now(DateTimeZone.UTC);
                var message = "";
                if (now.isBefore(endDateTime)) {
                  message = String.format("At %s, data is still being written", dateFormatter.print(now));
                } else {
                  message = String.format("At %s, data writes should have stopped on the segment ['%s' - '%s']",
                          dateFormatter.print(now), segmentStartStr, segmentEndStr);
                }
                System.out.printf(message + ", partition's element count %d.\n", r.get(0).getLongValue());
              });

      Thread.sleep(minCheckPeriodMs);
    }
  }
}
