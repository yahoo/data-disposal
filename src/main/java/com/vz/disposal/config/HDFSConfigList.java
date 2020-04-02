// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.config;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class HDFSConfigList implements BaseConfigList {
    @Getter
    @Setter
    private String hdfsNamenode;

    @Getter
    @Setter
    private List<HDFSConfigEntry> entries;
}
