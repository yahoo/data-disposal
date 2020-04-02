// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.config;

import lombok.Getter;
import lombok.Setter;


public class HiveConfigEntry extends ConfigEntry {
    @Getter
    @Setter
    private String database;

    @Getter
    @Setter
    private String table;

    @Getter
    @Setter
    private String partitionFilterKey;

    @Getter
    @Setter
    private Boolean deleteExternalData;

    @Getter
    @Setter
    private Boolean enableHivePartitionFilter;

    @Override
    public void setDateFormat(String dateFormat) {
        if (dateFormat == null) {
            throw new NullPointerException("dateFormat was null, but expected non-null");
        }
        super.setDateFormat(dateFormat);
    }
}
