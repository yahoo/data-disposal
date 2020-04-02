package com.vz.disposal.utils;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class UtilsTest {


    @DataProvider(name = "hiveEntries")
    public Object[][] hiveEntries() {
        return new Object[][]{
                {2019,  1, 23, 2, 48, ChronoUnit.HOURS  , 2019,  1, 21, 2}, // Typical case #0
                {2019,  1, 23, 2, 24, ChronoUnit.DAYS   , 2018, 12, 30, 2}, // Typical case #1
                {2019,  1, 23, 2,  1, ChronoUnit.MONTHS , 2018, 12, 23, 2}, // Typical case #2
                {2019,  1, 23, 2,  1, ChronoUnit.YEARS  , 2018,  1, 23, 2}, // Typical case #3
                {2019,  1, 23, 2,  1, ChronoUnit.DECADES, 2009,  1, 23, 2}, // Typical case #4
                {2019,  1, 23, 2,  0, ChronoUnit.DAYS   , 2019,  1, 23, 2}, // Typical case #5
                {2019,  1, 23, 2, -1, ChronoUnit.DAYS   , 2019,  1, 24, 2}, // Typical case #6
                {2019,  3, 12, 2,  1, ChronoUnit.MONTHS , 2019,  2, 12, 2}, // Leap year case #0
                {2016, 11,  6, 2,  1, ChronoUnit.YEARS  , 2015, 11,  6, 2}, // Leap year case #1
        };
    }

    @Test(dataProvider = "hiveEntries")
    public void testGetBeginningOfRetention(
            int actualYear        ,
            int actualMonth       ,
            int actualDay         ,
            int actualHour        ,
            int duration          ,
            ChronoUnit timeUnit   ,
            int expectedYear        ,
            int expectedMonth       ,
            int expectedDay         ,
            int expectedHour
    ) {
        ZonedDateTime endOfWindow = ZonedDateTime.of(
                actualYear, actualMonth, actualDay, actualHour, 0, 0, 0, ZoneOffset.UTC
        );

        Instant actual = Utils.getBeginningOfRetention(endOfWindow, duration, timeUnit);
        Instant expected = ZonedDateTime.of(
                expectedYear, expectedMonth, expectedDay, expectedHour, 0, 0, 0, ZoneOffset.UTC
        ).toInstant();

        Assert.assertEquals(actual, expected);
    }
}
