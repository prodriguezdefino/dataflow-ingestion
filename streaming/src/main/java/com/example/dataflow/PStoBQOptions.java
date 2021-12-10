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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Options supported by the pipeline.
 *
 * <p>
 * Inherits standard configuration options.
 */
public interface PStoBQOptions extends DataflowPipelineOptions {

  @Description("The Cloud Pub/Sub subscription to read from.")
  @Validation.Required
  ValueProvider<String> getInputSubscription();

  void setInputSubscription(ValueProvider<String> value);

  @Description(
          "The window duration in which data will be written. Defaults to 5m. "
          + "Allowed formats are: "
          + "Ns (for seconds, example: 5s), "
          + "Nm (for minutes, example: 12m), "
          + "Nh (for hours, example: 2h).")
  @Default.String("1m")
  String getInsertErrorWindowDuration();

  void setInsertErrorWindowDuration(String value);

  @Description("Local path to the AVRO schema to use.")
  @Validation.Required
  String getAvroSchemaFileLocation();

  void setAvroSchemaFileLocation(String value);

  @Description("Output BQ table FQN <project:dataset.table>")
  @Validation.Required
  ValueProvider<String> getOutputTableSpec();

  void setOutputTableSpec(ValueProvider<String> value);

  @Description("Includes an insert timestamp column in the table and populates with the insertion time.")
  @Default.Boolean(false)
  Boolean isIncludeInsertTimestamp();

  void setIncludeInsertTimestamp(Boolean value);
  
  @Description("Sets the write mode.")
  @Default.String("STREAMING_INSERTS")
  String getWriteMode();

  void setWriteMode(String value);
}
