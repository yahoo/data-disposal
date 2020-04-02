// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.config;

import lombok.Getter;
import lombok.Setter;


public class HDFSConfigEntry extends ConfigEntry {
    @Getter
    @Setter
    private String path;

    @Getter
    @Setter
    private HDFSRetentionType retentionType;

    @Getter
    @Setter
    private boolean recursive;
}
