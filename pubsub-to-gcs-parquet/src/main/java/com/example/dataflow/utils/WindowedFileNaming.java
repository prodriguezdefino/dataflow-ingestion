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

import java.util.Optional;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple file naming implementation that produces a Hive-partition-like path for the output files.
 */
public class WindowedFileNaming implements FileIO.Write.FileNaming {

  private static final Logger LOG = LoggerFactory.getLogger(WindowedFileNaming.class);

  private final ValueProvider<String> filePrefix;
  private final ValueProvider<String> fileSuffix;
  private final String exec;
  private Boolean includeMinutesInPath = true;

  public WindowedFileNaming(ValueProvider<String> filePrefix, ValueProvider<String> fileSuffix, String exec) {
    this.filePrefix = filePrefix;
    this.fileSuffix = fileSuffix;
    this.exec = exec;
  }

  public WindowedFileNaming withHourlyPaths() {
    this.includeMinutesInPath = false;
    return this;
  }

  public WindowedFileNaming cloneWithHourlyPaths() {
    WindowedFileNaming naming = new WindowedFileNaming(this.filePrefix, this.fileSuffix, this.exec);
    naming.includeMinutesInPath = false;
    return naming;
  }

  @Override
  public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
    String outputPath
            = Optional
                    .ofNullable(window)
                    .map(w -> w.maxTimestamp().toDateTime())
                    .map(time -> includeMinutesInPath
                    ? Utilities.buildPartitionedPathFromDatetime(time)
                    : Utilities.buildHourlyPartitionedPathFromDatetime(time))
                    .orElse("");

    StringBuilder fileNameSB = new StringBuilder(outputPath);

    if (!filePrefix.get().isEmpty()) {
      fileNameSB.append(filePrefix.get());
    }

    if (!includeMinutesInPath && window != null) {
      fileNameSB.append("-").append(Utilities.formatFilenameWindowComponent(window.maxTimestamp().toDateTime()));
    }

    fileNameSB.append("-pane-").append(pane.getIndex())
            .append("-shard-").append(shardIndex).append("-of-").append(numShards)
            .append("-exec-").append(exec);

    if (!fileSuffix.get().isEmpty()) {
      fileNameSB.append(fileSuffix.get());
    }

    if (!Compression.UNCOMPRESSED.equals(compression)) {
      fileNameSB.append(compression.name().toLowerCase());
    }

    LOG.debug("Windowed file name policy created: {}", fileNameSB.toString());
    return fileNameSB.toString();
  }

}
