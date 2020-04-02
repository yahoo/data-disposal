// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.utils;

import org.apache.hadoop.fs.FileStatus;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public abstract class TimePathFilter {
    protected final Instant beginningOfRetention;

    public TimePathFilter(ZonedDateTime timeOfRun, ChronoUnit granularity, int retentionDuration) {
        this.beginningOfRetention = Utils.getBeginningOfRetention(timeOfRun, retentionDuration, granularity);
    }

    public abstract boolean accept(FileStatus fileStatus);
}
