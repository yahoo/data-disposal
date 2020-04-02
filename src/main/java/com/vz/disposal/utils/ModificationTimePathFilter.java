// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.utils;

import org.apache.hadoop.fs.FileStatus;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class ModificationTimePathFilter extends TimePathFilter {
    public ModificationTimePathFilter(ZonedDateTime timeOfRun, ChronoUnit granularity, int retentionDuration) {
        super(timeOfRun, granularity, retentionDuration);
    }

    @Override
    public boolean accept(FileStatus fileStatus) {
        long modificationTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(fileStatus.getModificationTime());

        return modificationTimeSeconds < beginningOfRetention.getEpochSecond();
    }
}
