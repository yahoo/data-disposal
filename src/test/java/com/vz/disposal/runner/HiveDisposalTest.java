// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.runner;

import com.vz.disposal.config.HiveConfigEntry;
import com.vz.disposal.config.HiveConfigList;
import com.vz.disposal.utils.Utils;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.ExpectedExceptions;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.vz.disposal.runner.HiveDisposal.verifyDatePartitionExists;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HiveDisposalTest {

    @DataProvider(name = "hiveEntries")
    public Object[][] hiveEntries() throws HCatException {
        String db1 = "db1";
        String table1 = "table1";
        String partitionFilterKey = "key";

        Instant partitionDate = Utils.getBeginningOfRetention(Disposal.TIME_OF_RUN,2, ChronoUnit.MONTHS);
        String partitionDateString = new SimpleDateFormat("yyyy-MM-dd").format(Date.from(partitionDate));

        return new Object[][] {
                {true,  db1, table1, partitionFilterKey, false, false, 25, ChronoUnit.DAYS  , "yyyy-MM-dd", 1, 0, partitionDateString},
                {false, db1, table1, partitionFilterKey, false, false, 25, ChronoUnit.DAYS  , "yyyy-MM-dd", 1, 1, partitionDateString},
                {false, db1, table1, partitionFilterKey, false, true , 25, ChronoUnit.DAYS  , "yyyy-MM-dd", 1, 1, partitionDateString},
                {false, db1, table1, partitionFilterKey, true,  false, 25, ChronoUnit.DAYS  , "yyyy-MM-dd", 1, 1, partitionDateString},
        };
    }

    @DataProvider(name = "schemata")
    public Object[][] schemata() throws HCatException {

        HCatClient client = Mockito.mock(HCatClient.class);
        HCatTable  table  = Mockito.mock(HCatTable.class);
        List<HCatFieldSchema> mockColumns = new ArrayList<>();
        HCatFieldSchema column1  = Mockito.mock(HCatFieldSchema.class);
        HCatFieldSchema column2  = Mockito.mock(HCatFieldSchema.class);

        when(column1.getName()).thenReturn("trans_dt");
        when(column2.getName()).thenReturn("ticker");

        mockColumns.add(column1);
        mockColumns.add(column2);

        HiveConfigEntry configEntryMock1 = Mockito.mock(HiveConfigEntry.class);
        when(configEntryMock1.getDatabase()).thenReturn("whatever");
        when(configEntryMock1.getTable()).thenReturn("whatever");
        when(configEntryMock1.getPartitionFilterKey()).thenReturn("trans_dt");

        HiveConfigEntry configEntryMock2 = Mockito.mock(HiveConfigEntry.class);
        when(configEntryMock2.getDatabase()).thenReturn("whatever");
        when(configEntryMock2.getTable()).thenReturn("whatever");
        when(configEntryMock2.getPartitionFilterKey()).thenReturn("commit_time");

        when(client.getTable(configEntryMock1.getDatabase(), configEntryMock1.getTable())).thenReturn(table);
        when(client.getTable(configEntryMock2.getDatabase(), configEntryMock2.getTable())).thenReturn(table);

        when(table.getPartCols()).thenReturn(mockColumns);

        return new Object[][] {
                {client, configEntryMock1, true},
                {client, configEntryMock2, false}
        };
    }

    @Test(dataProvider = "hiveEntries")
    public void testHiveDisposal(
            boolean dryRun,
            String db,
            String table,
            String partitionFilterKey,
            boolean deleteExternalData,
            boolean enableHivePartitionFilter,
            int retentionDuration,
            ChronoUnit granularity,
            String format,
            int expectedSize,
            int dropExpectedCalls,
            String datestamp) throws HCatException {

        Instant filterDate      = Utils.getBeginningOfRetention(
            Disposal.TIME_OF_RUN,
            retentionDuration,
            granularity
        );
        String filterDateString = "\"" + formatInstant(filterDate, format) + "\"";

        HCatClient client = Mockito.mock(HCatClient.class);
        List<HCatPartition> partitions  = new ArrayList<>();
        HCatPartition partition  = Mockito.mock(HCatPartition.class);

        LinkedHashMap<String, String> partitionKeyValue = new LinkedHashMap<>();
        partitionKeyValue.put(partitionFilterKey, datestamp);
        when(partition.getPartitionKeyValMap()) .thenReturn(partitionKeyValue);

        partitions.add(partition);
        when(client.getPartitions(db, table)).thenReturn(partitions);
        when(client.listPartitionsByFilter(db, table, "key < " + filterDateString)).thenReturn(partitions);

        HiveConfigEntry entry = new HiveConfigEntry();
        entry.setDatabase(db);
        entry.setTable(table);
        entry.setPartitionFilterKey(partitionFilterKey);
        entry.setDeleteExternalData(deleteExternalData);
        entry.setEnableHivePartitionFilter(enableHivePartitionFilter);
        entry.setRetentionDuration(retentionDuration);
        entry.setGranularity(granularity);
        entry.setDateFormat(format);

        HiveConfigList conf = new HiveConfigList();
        List<HiveConfigEntry> theList = new ArrayList<>();
        conf.setEntries(theList);

        HiveDisposal runner = new HiveDisposal(conf, dryRun, client);
        List<Map<String, String>> upForDisposal = runner.dispose(entry);

        Assert.assertEquals(upForDisposal.size(), expectedSize);
        if (upForDisposal.size() == 1) {
            Assert.assertEquals(upForDisposal.get(0).get(partitionFilterKey), datestamp);
        }

        verify(client, atLeast(dropExpectedCalls)).dropPartitions(eq(db), eq(table), eq(partitionKeyValue), eq(true), eq(deleteExternalData));
        verify(client, atMost(dropExpectedCalls)).dropPartitions(eq(db), eq(table), eq(partitionKeyValue), eq(true), eq(deleteExternalData));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testHiveDisposalException() throws HCatException {

        HCatClient client = Mockito.mock(HCatClient.class);
        HCatTable   table = Mockito.mock(HCatTable.class);
        List<HCatFieldSchema> mockColumns = new ArrayList<>();
        HCatFieldSchema column1  = Mockito.mock(HCatFieldSchema.class);
        HCatFieldSchema column2  = Mockito.mock(HCatFieldSchema.class);

        when(column1.getName()).thenReturn("run_date");
        when(column2.getName()).thenReturn("ticker");

        mockColumns.add(column1);
        mockColumns.add(column2);

        when(client.getTable("whatever", "whatever")).thenReturn(table);
        when(table.getPartCols()).thenReturn(mockColumns);

        HiveConfigEntry entry = new HiveConfigEntry();
        entry.setDatabase("whatever");
        entry.setTable("whatever");
        entry.setPartitionFilterKey("DOES NOT EXIST");
        entry.setDeleteExternalData(false);
        entry.setEnableHivePartitionFilter(false);
        entry.setRetentionDuration(-1);
        entry.setGranularity(ChronoUnit.MICROS);
        entry.setDateFormat("yyyy-mm-dd");

        HiveConfigList conf = new HiveConfigList();
        List<HiveConfigEntry> theList = new ArrayList<>();
        theList.add(entry);
        conf.setEntries(theList);

        HiveDisposal disposal = new HiveDisposal(conf, true, client);
    }

    @Test(dataProvider = "schemata")
    public void testVerifyDatePartitionExists(
            HCatClient client,
            HiveConfigEntry mockConfigEntry,
            boolean expected) {
        Assert.assertEquals(verifyDatePartitionExists(client, mockConfigEntry), expected);
    }

    private static String formatInstant(Instant instantToFormat, String pattern) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(pattern)
                .withZone(ZoneOffset.UTC);

        return dtf.format(instantToFormat);
    }
}
