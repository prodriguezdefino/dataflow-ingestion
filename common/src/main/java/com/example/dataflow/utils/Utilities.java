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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Locale;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

/**
 * A collection of static methods for Date manipulation.
 */
public class Utilities {

  private static final String OUTPUT_PATH_MINUTE_WINDOW = "YYYY/MM/DD/HH/mm/";
  private static final String OUTPUT_PATH_HOURLY_WINDOW = "YYYY/MM/DD/HH/";
  private static final DateTimeFormatter OUTPUT_HOURLY_WINDOW_FILENAME_COMPONENT = ISODateTimeFormat.basicDateTime();
  private static final DateTimeFormatter YEAR = DateTimeFormat.forPattern("YYYY");
  private static final DateTimeFormatter MONTH = DateTimeFormat.forPattern("MM");
  private static final DateTimeFormatter DAY = DateTimeFormat.forPattern("dd");
  private static final DateTimeFormatter HOUR = DateTimeFormat.forPattern("HH");
  private static final DateTimeFormatter MINUTE = DateTimeFormat.forPattern("mm");

  /**
   *
   * @param time
   * @return
   */
  public static String buildPartitionedPathFromDatetime(DateTime time) {
    return OUTPUT_PATH_MINUTE_WINDOW
            .replace("YYYY", YEAR.print(time))
            .replace("MM", MONTH.print(time))
            .replace("DD", DAY.print(time))
            .replace("HH", HOUR.print(time))
            .replace("mm", MINUTE.print(time));
  }

  /**
   *
   * @param time
   * @return
   */
  public static String buildHourlyPartitionedPathFromDatetime(DateTime time) {
    return OUTPUT_PATH_HOURLY_WINDOW
            .replace("YYYY", YEAR.print(time))
            .replace("MM", MONTH.print(time))
            .replace("DD", DAY.print(time))
            .replace("HH", HOUR.print(time))
            .replace("mm", MINUTE.print(time));
  }

  /**
   * Formats the provided time to the format expected for the window component of the filename.
   *
   * @param time
   * @return
   */
  public static String formatFilenameWindowComponent(DateTime time) {
    return "w" + time.toString(OUTPUT_HOURLY_WINDOW_FILENAME_COMPONENT);
  }

  /**
   * Parses a duration from a period formatted string. Values are accepted in the following formats:
   *
   * <p>
   * Formats Ns - Seconds. Example: 5s<br>
   * Nm - Minutes. Example: 13m<br>
   * Nh - Hours. Example: 2h
   *
   * <pre>
   * parseDuration(null) = NullPointerException()
   * parseDuration("")   = Duration.standardSeconds(0)
   * parseDuration("2s") = Duration.standardSeconds(2)
   * parseDuration("5m") = Duration.standardMinutes(5)
   * parseDuration("3h") = Duration.standardHours(3)
   * </pre>
   *
   * @param value The period value to parse.
   * @return The {@link Duration} parsed from the supplied period string.
   */
  public static Duration parseDuration(String value) {
    checkNotNull(value, "The specified duration must be a non-null value!");

    PeriodParser parser = new PeriodFormatterBuilder()
            .appendSeconds()
            .appendSuffix("s")
            .appendMinutes()
            .appendSuffix("m")
            .appendHours()
            .appendSuffix("h")
            .toParser();

    MutablePeriod period = new MutablePeriod();
    parser.parseInto(period, value, 0, Locale.getDefault());

    Duration duration = period.toDurationFrom(new DateTime(0));
    checkArgument(duration.getMillis() > 0, "The window duration must be greater than 0!");

    return duration;
  }
}
