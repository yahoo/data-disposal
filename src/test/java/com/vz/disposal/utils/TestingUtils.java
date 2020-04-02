// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.utils;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;

public class TestingUtils {
    public static SimpleDateFormat getFormatter(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        sdf.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
        return sdf;
    }
}
