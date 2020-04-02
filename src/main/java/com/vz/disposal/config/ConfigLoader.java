// Copyright Verizon Media
// Licensed under the terms of the Apache 2.0 license. Please see LICENSE file in the project root for terms.

package com.vz.disposal.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;


public class ConfigLoader<T> {
    private static final Log LOG = LogFactory.getLog(ConfigLoader.class);

    public T loadConfig(String file, Class<T> clazz) {
        T config = null;
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            config = mapper.readValue(new File(file), clazz);
        } catch (IOException e) {
            LOG.error("Unable to load config file: " + file, e);
            System.exit(-1);
        }

        return config;
    }
}
