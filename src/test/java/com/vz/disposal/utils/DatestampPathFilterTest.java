// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.utils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;

import static org.mockito.Mockito.when;

public class DatestampPathFilterTest {

    static final ZonedDateTime TIME_OF_TEST_RUN = ZonedDateTime.now(ZoneOffset.UTC);

    private FileStatus mockStatus(Path path) {
        FileStatus fs = Mockito.mock(FileStatus.class);
        when(fs.getPath()).thenReturn(path);
        return fs;
    }

    @DataProvider(name = "acceptCases")
    public Object[][] acceptCases() {
        String pattern = "yyyyMMdd";
        SimpleDateFormat formatter = TestingUtils.getFormatter(pattern);
        String nowminus30 = new DateTimeFormatterBuilder().appendPattern(pattern).toFormatter()
                .withZone(ZoneId.of("UTC")).format(Instant.now().minus(30, ChronoUnit.DAYS));
        return new Object[][] {
                {ChronoUnit.DAYS, 30, 10, formatter, mockStatus(new Path("/somepath/2017-05-23/a")), true},
                {ChronoUnit.DAYS, 30, 10, formatter, mockStatus(new Path("/somepath/2017-05-23")), true},
                {ChronoUnit.DAYS, 31, 10, formatter, mockStatus(new Path("/somepath/" + nowminus30 + "/b")), false},
                {ChronoUnit.DAYS, 30, 10, formatter, mockStatus(new Path("/somepath/" + nowminus30 + "/c")), true},
                {ChronoUnit.DAYS, 29, 10, formatter, mockStatus(new Path("/somepath/" + nowminus30 + "/d")), true},
                {ChronoUnit.DAYS, 15, 10, formatter, mockStatus(new Path("/somepath/" + nowminus30 + "/e")), true},
                {ChronoUnit.DAYS, 60, 10, formatter, mockStatus(new Path("/somepath/" + nowminus30 + "/f")), false},
                {ChronoUnit.DAYS, 15, 20, formatter, mockStatus(new Path("/somepath/somepath2/" + nowminus30 + "/g")), true},
                {ChronoUnit.DAYS, 15, 20, formatter, mockStatus(new Path("/somepath/somepath2/" + nowminus30 + "220000/h")), true},
        };
    }


    @Test(dataProvider = "acceptCases")
    public void testAccept(
            ChronoUnit granularity,
            int retentionDuration,
            int indexOfDatestamp,
            SimpleDateFormat formatter,
            FileStatus fileStatus,
            boolean expected) {

        DatestampPathFilter filter = new DatestampPathFilter(
                TIME_OF_TEST_RUN,
                granularity,
                retentionDuration,
                indexOfDatestamp,
                formatter);

        Assert.assertEquals(filter.accept(fileStatus), expected);
    }
}
