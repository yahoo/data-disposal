// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal;

import com.vz.disposal.runner.Disposal;
import com.vz.disposal.runner.HDFSDisposal;
import com.vz.disposal.runner.HiveDisposal;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataDisposal {
    private static final Log LOG = LogFactory.getLog(DataDisposal.class);

    public static final String HDFS_CONF = "hdfs_conf";
    public static final String HIVE_CONF = "hive_conf";
    public static final String DRY_RUN = "dry_run";

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();

        Option hdfsConf = Option.builder()
                .longOpt(HDFS_CONF)
                .hasArg()
                .desc("Config for HDFS Disposal")
                .optionalArg(true)
                .build();

        options.addOption(hdfsConf);

        Option hiveConf = Option.builder()
                .longOpt(HIVE_CONF)
                .hasArg()
                .desc("Config for Hive Disposal")
                .optionalArg(true)
                .build();

        options.addOption(hiveConf);

        Option dryRun = Option.builder()
                .longOpt(DRY_RUN)
                .desc("Enable dry run")
                .optionalArg(true)
                .build();

        options.addOption(dryRun);
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }
    
    public static void main(String[] args) throws ParseException, IOException {
        CommandLine cmdArgs = parseArgs(args);
        List<Disposal> disposals = new ArrayList<>();
        boolean dryRun = cmdArgs.hasOption(DRY_RUN);

        LOG.info("Data disposal started with command: " + String.join(" ", args));

        if (cmdArgs.hasOption(HIVE_CONF)) {
            disposals.add(new HiveDisposal(cmdArgs.getOptionValue(HIVE_CONF), dryRun));
        }

        if (cmdArgs.hasOption(HDFS_CONF)) {
            disposals.add(new HDFSDisposal(cmdArgs.getOptionValue(HDFS_CONF), dryRun));
        }

        disposals.forEach(Disposal::run);
    }
}
