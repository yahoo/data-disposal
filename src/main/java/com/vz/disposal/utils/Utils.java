// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class Utils {
    public static Instant getBeginningOfRetention(ZonedDateTime endOfWindow, int retentionDuration, ChronoUnit granularity) {
        ZonedDateTime currentDateTime = endOfWindow.withZoneSameInstant(ZoneOffset.UTC);
        return currentDateTime.minus(retentionDuration, granularity).toInstant();
    }
}
