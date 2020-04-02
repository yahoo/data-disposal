// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.utils;

import org.apache.hadoop.fs.FileStatus;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static org.mockito.Mockito.when;

public class ModificationTimePathFilterTest {
    private static final long MILLI_IN_SECOND = 1000;
    static final ZonedDateTime TIME_OF_TEST_RUN = ZonedDateTime.now(ZoneOffset.UTC);


    @DataProvider(name = "acceptCases")
    public Object[][] acceptCases() throws IOException {
        long nowminus5 = Instant.now().minus(5, ChronoUnit.DAYS).getEpochSecond() * MILLI_IN_SECOND;

        FileStatus fileStatus1 = Mockito.mock(FileStatus.class);
        when(fileStatus1.getModificationTime()).thenReturn(Instant.now().getEpochSecond() * MILLI_IN_SECOND);

        FileStatus fileStatus2 = Mockito.mock(FileStatus.class);
        when(fileStatus2.getModificationTime()).thenReturn(nowminus5);


        return new Object[][]{
                {ChronoUnit.DAYS, 5, fileStatus1, false},
                {ChronoUnit.DAYS, 5, fileStatus2, false},
                {ChronoUnit.DAYS, 3, fileStatus2, true},
                {ChronoUnit.DAYS, 4, fileStatus2, true},
                {ChronoUnit.DAYS, 300, fileStatus2, false},
                {ChronoUnit.DAYS, 0, fileStatus1, false},
                {ChronoUnit.DAYS, 0, fileStatus2, true},
                {ChronoUnit.DAYS, -2, fileStatus1, true},
        };
    }


    @Test(dataProvider = "acceptCases")
    public void testAccept(
            ChronoUnit granularity,
            int retentionDuration,
            FileStatus fileStatus,
            boolean expected) {

        ModificationTimePathFilter filter = new ModificationTimePathFilter(
                TIME_OF_TEST_RUN,
                granularity,
                retentionDuration);

        Assert.assertEquals(filter.accept(fileStatus), expected);
    }
}
