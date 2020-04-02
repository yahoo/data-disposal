// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.config;

import lombok.Getter;
import lombok.Setter;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

public abstract class ConfigEntry {
    @Getter
    @Setter
    private int retentionDuration;

    @Getter
    @Setter
    private ChronoUnit granularity;

    @Getter
    private SimpleDateFormat dateFormat;

    public void setDateFormat(String dateFormat) {
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.dateFormat.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
    }
}
