// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class HiveConfigList implements BaseConfigList {
    @Getter
    @Setter
    private Path hiveSite;

    @Getter
    @Setter
    private List<HiveConfigEntry> entries;
}
