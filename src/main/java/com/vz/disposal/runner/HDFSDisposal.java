// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.runner;

import com.vz.disposal.config.ConfigLoader;
import com.vz.disposal.config.HDFSConfigEntry;
import com.vz.disposal.config.HDFSConfigList;
import com.vz.disposal.config.HDFSRetentionType;
import com.vz.disposal.utils.DatestampPathFilter;
import com.vz.disposal.utils.ModificationTimePathFilter;
import com.vz.disposal.utils.TimePathFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class HDFSDisposal extends Disposal<HDFSConfigList, HDFSConfigEntry> {
    private static final Log LOG = LogFactory.getLog(HDFSDisposal.class);
    public static final String TIMESTAMP_LOCATOR = "%s";

    private final FileSystem fs;

    /**
     * This should only be used for unit testing.
     */
    protected HDFSDisposal(HDFSConfigList config, boolean dryRun, FileSystem mockfs) {
        super(config, dryRun);

        this.fs = mockfs;
    }

    public HDFSDisposal(String confFile, boolean dryRun) throws IOException {
        super(new ConfigLoader<HDFSConfigList>().loadConfig(confFile, HDFSConfigList.class), dryRun);
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        this.fs = FileSystem.get(URI.create(config.getHdfsNamenode()), conf);
    }

    @Override
    protected List dispose(HDFSConfigEntry entry) {
        List<Path> upForDisposal = new ArrayList<>();

        String globPath = entry.getPath();

        ChronoUnit granularity = entry.getGranularity();
        int retentionDuration = entry.getRetentionDuration();
        TimePathFilter pathFilter = null;

        HDFSRetentionType retentionType = entry.getRetentionType();
        if (retentionType.equals(HDFSRetentionType.MODIFICATION_TIME)) {
            pathFilter = new ModificationTimePathFilter(TIME_OF_RUN, granularity, retentionDuration);
        } else if (entry.getRetentionType().equals(HDFSRetentionType.PATH_DATE)) {
            int indexOfDatestamp = globPath.indexOf(TIMESTAMP_LOCATOR);
            globPath = globPath.replace(TIMESTAMP_LOCATOR, "*");
            pathFilter = new DatestampPathFilter(
                    TIME_OF_RUN,
                    granularity,
                    retentionDuration,
                    indexOfDatestamp,
                    entry.getDateFormat()
            );
        }
        FileStatus[] dirs;
        try {
            dirs = fs.globStatus(new Path(globPath));
        } catch (IOException e) {
            LOG.error("Glob status failed on path: " + globPath, e);
            // Removing exit for now. probably should add config param
            // System.exit(-1);
            dirs = null;
        }

        if (dirs == null || dirs.length == 0) {
            LOG.info("No directories to remove for: " + globPath);
            return upForDisposal;
        }

        upForDisposal = Arrays.asList(dirs).stream()
                .filter(pathFilter::accept)
                .map(FileStatus::getPath)
                .collect(Collectors.toList());

        LOG.info(upForDisposal.size() + " paths up for disposal: " + upForDisposal);

        if (!dryRun) {
            LOG.info("Disposal for " + globPath);
            upForDisposal.stream().forEach(
                    dir -> {
                        try {
                            fs.delete(dir, entry.isRecursive());
                        } catch (IOException e) {
                            LOG.error("Delete failed on path: " + dir, e);
                        }
                    });
        }

        return upForDisposal;
    }
}
