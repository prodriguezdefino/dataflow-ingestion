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
public interface GCSParquetToCSVOptions extends DataflowPipelineOptions {

  @Description("Input location for the AVRO files.")
  @Validation.Required
  ValueProvider<String> getInputLocation();

  void setInputLocation(ValueProvider<String> value);

  @Description("Local path to the AVRO schema to use.")
  @Validation.Required
  String getAvroSchemaFileLocation();

  void setAvroSchemaFileLocation(String value);

  @Description("Number of files per eye color.")
  @Default.Integer(1)
  Integer getNumShards();

  void setNumShards(Integer value);

  @Description("Output location for the CSV files.")
  @Validation.Required
  ValueProvider<String> getOutputLocation();

  void setOutputLocation(ValueProvider<String> value);

}
