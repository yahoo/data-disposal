// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class DatestampPathFilter extends TimePathFilter {
    private static final Log LOG = LogFactory.getLog(DatestampPathFilter.class);

    private final ParsePosition indexOfDatestamp;
    private final SimpleDateFormat pathDateFormat;

    public DatestampPathFilter(
            ZonedDateTime timeOfRun,
            ChronoUnit granularity,
            int retentionDuration,
            int indexOfDatestamp,
            SimpleDateFormat pathDateFormat
    ) {
        super(timeOfRun, granularity, retentionDuration);
        this.indexOfDatestamp = new ParsePosition(indexOfDatestamp);
        this.pathDateFormat = pathDateFormat;
    }

    @Override
    public boolean accept(FileStatus fileStatus) {
        // Need to save index because the parse method modifies the value.
        int startIndex = indexOfDatestamp.getIndex();
        int dateLength = this.pathDateFormat.toPattern().length();
        String path = fileStatus.getPath().toString();
        int pathExpectedLength = startIndex + dateLength;

        if (path.length() < pathExpectedLength) {
            LOG.error("Path " + path + " is shorter than expected: " + pathExpectedLength);
            return false;
        }

        String date = path.substring(startIndex, pathExpectedLength);
        Date pathDate = null;

        try {
            pathDate = pathDateFormat.parse(date);
        } catch (ParseException pe) {
            LOG.error(pe);
        }

        if (pathDate == null) {
            LOG.error("Unable to parse date: " + date + " from path " + path);
            return false;
        }

        return pathDate.toInstant().isBefore(beginningOfRetention);
    }

}
