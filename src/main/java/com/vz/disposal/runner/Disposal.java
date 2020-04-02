// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.runner;

import com.vz.disposal.config.BaseConfigList;
import com.vz.disposal.config.ConfigEntry;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

public abstract class Disposal<T extends BaseConfigList, U extends ConfigEntry> {
    protected final T config;
    protected final boolean dryRun;
    protected static final ZonedDateTime TIME_OF_RUN = ZonedDateTime.now(ZoneOffset.UTC);

    protected Disposal(T config, boolean dryRun) {
        this.config = config;
        this.dryRun = dryRun;
    }

    public void run() {
        config.getEntries().parallelStream().forEach(entry -> this.dispose((U) entry));
    }

    protected abstract List dispose(U entry);
}
