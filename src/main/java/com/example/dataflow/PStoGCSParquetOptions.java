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
public interface PStoGCSParquetOptions extends DataflowPipelineOptions {

  @Description("The Cloud Pub/Sub subscription to read from.")
  @Validation.Required
  ValueProvider<String> getInputSubscription();

  void setInputSubscription(ValueProvider<String> value);

  @Description("The directory to output files to. Must end with a slash.")
  @Validation.Required
  ValueProvider<String> getOutputDirectory();

  void setOutputDirectory(ValueProvider<String> value);

  @Description("The directory to output temp files to, used when composing files. Must end with a slash.")
  @Default.String("NA")
  ValueProvider<String> getComposeTempDirectory();

  void setComposeTempDirectory(ValueProvider<String> value);

  @Description("The filename prefix of the files to write to.")
  @Default.String("prefix-name")
  ValueProvider<String> getOutputFilenamePrefix();

  void setOutputFilenamePrefix(ValueProvider<String> value);

  @Description("The suffix of the files to write.")
  @Default.String(".parquet")
  ValueProvider<String> getOutputFilenameSuffix();

  void setOutputFilenameSuffix(ValueProvider<String> value);

  @Description("The maximum number of output shards produced when writing.")
  @Default.Integer(400)
  Integer getNumShards();

  void setNumShards(Integer value);

  @Description(
          "The window duration in which data will be written. Defaults to 5m. "
          + "Allowed formats are: "
          + "Ns (for seconds, example: 5s), "
          + "Nm (for minutes, example: 12m), "
          + "Nh (for hours, example: 2h).")
  @Default.String("5m")
  String getWindowDuration();

  void setWindowDuration(String value);

  @Description("The Parquet Write Temporary Directory. Must end with /")
  @Validation.Required
  ValueProvider<String> getTempDirectory();

  void setTempDirectory(ValueProvider<String> value);

  @Description("Creates a SUCCESS file once all data on a window has been received")
  @Default.Boolean(true)
  Boolean getCreateSuccessFile();

  void setCreateSuccessFile(Boolean value);

  @Description("Enables composition of multiple small files into bigger ones (Parquet support included in this pipeline)")
  @Default.Boolean(false)
  Boolean getComposeSmallFiles();

  void setComposeSmallFiles(Boolean value);

  @Description("Number of files to be written after compose stage in a particular window (less files per window, bigger file sizes).")
  @Default.Integer(10)
  Integer getComposeShards();

  void setComposeShards(Integer value);

  @Description("Cleans all files part after composing them (Parquet support included in this pipeline)")
  @Default.Boolean(true)
  Boolean getCleanComposePartFiles();

  void setCleanComposePartFiles(Boolean value);

  @Description("Local path to the AVRO schema to use.")
  @Validation.Required
  String getAvroSchemaFileLocation();

  void setAvroSchemaFileLocation(String value);

}
