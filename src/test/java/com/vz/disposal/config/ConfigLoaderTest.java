// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.config;

import com.vz.disposal.utils.TestingUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ConfigLoaderTest {

    private static final List<Map> hdfsEntries = new LinkedList(Arrays.asList(
            new Map[]{
                    new HashMap<String, Object>() {
                        {
                            put("path", "hdfs://namenode:8020/somepath/dir/date=%s");
                            put("retentionDuration", 14);
                            put("granularity", ChronoUnit.DAYS);
                            put("retentionType", HDFSRetentionType.PATH_DATE);
                            put("dateFormat", TestingUtils.getFormatter("yyyy-MM-dd"));
                            put("recursive", true);
                        }
                    },
                    new HashMap<String, Object>() {
                        {
                            put("path", "hdfs://namenode:8020/somepath/metadata/somepath");
                            put("retentionDuration", 14);
                            put("granularity", ChronoUnit.HOURS);
                            put("retentionType", HDFSRetentionType.MODIFICATION_TIME);
                            put("dateFormat", null);
                            put("recursive", false);
                        }
                    }
            }
    ));

    private static final List<Map> hiveEntries = new LinkedList(Arrays.asList(

            new Map[]{
                    new HashMap<String, Object>() {
                        {
                            put("table", "users");
                            put("database", "somename");
                            put("retentionDuration", 14);
                            put("granularity", ChronoUnit.DAYS);
                            put("dateFormat", TestingUtils.getFormatter("yyyy-MM"));
                            put("partitionFilterKey", "date");
                            put("deleteExternalData", true);
                        }
                    },
                    new HashMap<String, Object>() {
                        {
                            put("table", "comments");
                            put("database", "somename2");
                            put("retentionDuration", 14);
                            put("granularity", ChronoUnit.WEEKS);
                            put("dateFormat", TestingUtils.getFormatter("yyyy-MM"));
                            put("partitionFilterKey", "date");
                            put("deleteExternalData", false);
                        }
                    }
            }
    ));

    @DataProvider(name = "hdfsEntries")
    public Object[][] hdfsEntries() {
         List<HDFSConfigEntry> entries =  new LinkedList(new ConfigLoader<HDFSConfigList>()
                 .loadConfig("./src/test/resources/HDFSConfigExample.yaml", HDFSConfigList.class)
                 .getEntries());
         return entries.stream()
                .map(entry -> new Object[] {entry, hdfsEntries.remove(0), "2019-03-17"})
                .toArray(Object[][]::new);
    }

    @DataProvider(name = "hiveEntries")
    public Object[][] hiveEntries() {
        List<HiveConfigEntry> entries =  new LinkedList(new ConfigLoader<HiveConfigList>()
                .loadConfig("./src/test/resources/HiveConfigExample.yaml", HiveConfigList.class)
                .getEntries());
        return entries.stream()
                .map(entry -> new Object[] {entry, hiveEntries.remove(0), "2019-05"})
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "hdfsEntries")
    public void testHDFSConfigLoad(HDFSConfigEntry actual, Map<String, Object> expected, String date) throws ParseException {
        Assert.assertEquals(actual.getPath(), expected.get("path"));
        Assert.assertEquals(actual.getRetentionDuration(), expected.get("retentionDuration"));
        Assert.assertEquals(actual.getGranularity(), expected.get("granularity"));
        Assert.assertEquals(actual.getRetentionType(), expected.get("retentionType"));
        Assert.assertEquals(actual.isRecursive(), expected.get("recursive"));
        if (expected.get("dateFormat") != null) {
            Assert.assertEquals(actual.getDateFormat().parse(date).toString(), ((SimpleDateFormat) expected.get("dateFormat")).parse(date).toString());
        }
    }

    @Test(dataProvider = "hiveEntries")
    public void testHiveConfigLoad(HiveConfigEntry actual, Map<String, Object> expected, String date) throws Exception {
        Assert.assertEquals(actual.getTable(), expected.get("table"));
        Assert.assertEquals(actual.getDatabase(), expected.get("database"));
        Assert.assertEquals(actual.getRetentionDuration(), expected.get("retentionDuration"));
        Assert.assertEquals(actual.getGranularity(), expected.get("granularity"));
        Assert.assertEquals(actual.getDateFormat().parse(date).toString(), ((SimpleDateFormat) expected.get("dateFormat")).parse(date).toString());
        Assert.assertEquals(actual.getPartitionFilterKey(), expected.get("partitionFilterKey"));
        Assert.assertEquals(actual.getDeleteExternalData(), expected.get("deleteExternalData"));
    }

    @Test
    public void testHDFSConstants() {
        HDFSConfigList config = new ConfigLoader<HDFSConfigList>()
                .loadConfig("./src/test/resources/HDFSConfigExample.yaml", HDFSConfigList.class);
        Assert.assertEquals(config.getHdfsNamenode(), "hdfs://namenode:8020");
    }
}
