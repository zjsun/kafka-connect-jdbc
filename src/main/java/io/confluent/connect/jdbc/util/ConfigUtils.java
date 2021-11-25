/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utilities for configuration properties.
 */
public class ConfigUtils {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * Get the connector's name from the configuration.
     *
     * @param connectorProps the connector properties
     * @return the concatenated string with delimiters
     */
    public static String connectorName(Map<?, ?> connectorProps) {
        Object nameValue = connectorProps.get("name");
        return nameValue != null ? nameValue.toString() : null;
    }


    // datav fix
    public static final String DKE_MODE_DEFAULT = "stream";
    public static final String DKE_MODE_CONFIG = "dke.mode";
    private static final String DKE_MODE_DOC = "SCDF Mode: stream(default)/task";
    private static final String DKE_MODE_DISPLAY = "DKE Mode";
    private static final String DKE_GROUP = "DKE";
    private static final String DKE_MODE_STREAM = "stream";
    private static final String DKE_MODE_TASK = "task";

    public static ConfigDef configDkeMode(ConfigDef config) {
        log.info("Add dke.mode config ...");
        config.define(
                DKE_MODE_CONFIG,
                ConfigDef.Type.STRING, DKE_MODE_DEFAULT,
                ConfigDef.ValidString.in(DKE_MODE_STREAM, DKE_MODE_TASK),
                ConfigDef.Importance.LOW,
                DKE_MODE_DOC,
                DKE_GROUP,
                Integer.MAX_VALUE,
                ConfigDef.Width.SHORT,
                DKE_MODE_DISPLAY
        );
        return config;
    }

    public static boolean isDkeTaskMode(AbstractConfig config) {
        return StringUtils.equalsIgnoreCase(DKE_MODE_TASK, config.getString(DKE_MODE_CONFIG));
    }
}
