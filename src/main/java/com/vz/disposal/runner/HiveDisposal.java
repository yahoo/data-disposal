// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.runner;


import com.vz.disposal.config.ConfigLoader;
import com.vz.disposal.config.HiveConfigEntry;
import com.vz.disposal.config.HiveConfigList;
import com.vz.disposal.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import java.text.ParseException;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Date;

public class HiveDisposal extends Disposal<HiveConfigList, HiveConfigEntry> {
    private static final Log LOG = LogFactory.getLog(HiveDisposal.class);
    private final HCatClient hcatClient;

    /**
     * This should only be used for unit testing.
     */
    protected HiveDisposal(HiveConfigList configList, boolean dryRun, HCatClient hCatClient) {
        super(configList, dryRun);

        this.hcatClient = hCatClient;
        validateConfig();
    }

    public HiveDisposal(String confFile, boolean dryRun) {
        super(new ConfigLoader<HiveConfigList>().loadConfig(confFile, HiveConfigList.class), dryRun);

        Configuration conf = new Configuration();
        try {
            hcatClient = HCatClient.create(conf);
            validateConfig();
        } catch (HCatException | IllegalStateException exception) {
            throw new IllegalStateException("Failed to create HCatClient", exception);
        }
    }

    @Override
    protected List<Map<String, String>> dispose(HiveConfigEntry entry) {
        List<Map<String, String>> upForDisposal = new ArrayList<>();

        Instant beginningOfRetention = Utils.getBeginningOfRetention(TIME_OF_RUN, entry.getRetentionDuration(), entry.getGranularity());
        String databaseTable = entry.getDatabase() + "." + entry.getTable();

        upForDisposal = listPartitions(entry, beginningOfRetention);

        if (upForDisposal.isEmpty()) {
            return upForDisposal;
        }

        if (!dryRun) {
            HashSet<HashMap<String, String>> topLevelPartitions = new HashSet<>();
            String datePartitionKey = entry.getPartitionFilterKey();
            boolean dropIfExists = true;

            upForDisposal.stream().forEach(map -> {
                HashMap<String, String> datePartition = new HashMap<>();
                datePartition.put(datePartitionKey, map.get(datePartitionKey));
                topLevelPartitions.add(datePartition);
            });

            synchronized (hcatClient) {
                topLevelPartitions.stream().forEach(partition -> {
                    LOG.info("Running method 'hcatClient.dropPartitions(" +
                                    entry.getDatabase() + "," +
                                    entry.getTable()    + "," +
                                    partition           + "," +
                                    dropIfExists        + "," +
                                    entry.getDeleteExternalData() + ")'.");
                    try {
                        hcatClient.dropPartitions(
                                entry.getDatabase(),
                                entry.getTable(),
                                partition,
                                dropIfExists,
                                entry.getDeleteExternalData());
                    } catch (HCatException e) {
                        LOG.error("An exception occurred dropping partitions for filter: " + partition, e);
                    }
                });
            }
        }
        return upForDisposal;
    }

    private List<Map<String, String>> listPartitions(HiveConfigEntry confEntry, Instant retentionStartTimestamp) {
        String databaseTable = confEntry.getDatabase() + "." + confEntry.getTable();
        List<HCatPartition> partitions = new ArrayList<>();
        List<Map<String, String>> partitionsToDispose = new ArrayList<>();
        boolean enableHivePartitionFilter = confEntry.getEnableHivePartitionFilter() == null ?
                false :
                confEntry.getEnableHivePartitionFilter();

        try {
            if (enableHivePartitionFilter) {
                synchronized (hcatClient) {
                    String filter = confEntry.getPartitionFilterKey() +
                            " < " +
                            "\"" + confEntry.getDateFormat().format(Date.from(retentionStartTimestamp)) + "\"";

                    LOG.info("Retrieving partitions where " + filter);

                    partitions = hcatClient.listPartitionsByFilter(
                            confEntry.getDatabase(),
                            confEntry.getTable(),
                            filter
                    );
                }
            } else {
                synchronized (hcatClient) {
                    partitions = hcatClient.getPartitions(confEntry.getDatabase(), confEntry.getTable());
                }
            }
        } catch (HCatException e) {
            LOG.error("Unable to get partitions for: " + databaseTable, e);
        }


        partitions.stream()
                .forEach(
                        partition -> {
                            Map<String, String> keys = partition.getPartitionKeyValMap();
                            Instant partitionTime;
                            try {
                                partitionTime =
                                        confEntry.getDateFormat().parse(keys.get(confEntry.getPartitionFilterKey())).toInstant();
                            } catch (ParseException e) {
                                LOG.error("Cannot parse timestamp: " + keys + " from " + databaseTable);
                                throw new IllegalArgumentException(e);
                            }
                            if (partitionTime.getEpochSecond() <= retentionStartTimestamp.getEpochSecond()) {
                                partitionsToDispose.add(keys);
                            }
                        }
                );
        LOG.info(partitionsToDispose.size() + " partition(s) from " + databaseTable + " up for disposal: " + partitionsToDispose);

        return partitionsToDispose;
    }

    public static boolean verifyDatePartitionExists(HCatClient client, HiveConfigEntry config)
            throws IllegalStateException {
        String database = config.getDatabase(),
               table = config.getTable(),
               datePartitionKey = config.getPartitionFilterKey();

        List<HCatFieldSchema> partitionColumns;

        synchronized (client) {
            try {
                partitionColumns = client.getTable(database, table).getPartCols();
            } catch (HCatException hce) {
                   throw new IllegalStateException("Error occurred getting list of partition columns: ", hce);
            }
        }

        return partitionColumns.parallelStream().filter(
                   column -> column.getName().equals(datePartitionKey)
               ).count() > 0;
    }

    /**
     * Helper method invoked by both constructors to validate the configuration of the new HiveDisposal object.
     * Mainly exists to avoid code duplication.
     */

    protected void validateConfig() {
        this.config.getEntries().parallelStream().forEach(hiveConfigEntry -> {
            HiveConfigEntry entry = (HiveConfigEntry) hiveConfigEntry;
            if (entry.isValidationEnabled() && !verifyDatePartitionExists(hcatClient, entry)) {
                throw new IllegalStateException(
                        "Failed to detect date partition column " + entry.getPartitionFilterKey() + " in " +
                        entry.getDatabase() + "." + entry.getTable() + ". Please review the table's schema " +
                        "and adjust if necessary."
                );
            }
        });
    }
}
