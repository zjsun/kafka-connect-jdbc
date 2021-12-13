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

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.rangeClosed;

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

    public static Map<String, List<String>> breakDownHeaders(String headers) {
        return breakDownMultiValuePairs(headers, "(?<!\\\\),", ":")
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, entry ->
                        entry.getValue().stream().map(value ->
                                value.replaceAll("\\\\,", ","))
                                .collect(toList())));
    }

    public static Map<String, List<String>> breakDownQueryParams(String queryParams) {
        return breakDownMultiValuePairs(queryParams, "&", "=");
    }

    public static Map<String, String> breakDownMap(String mapString) {
        return breakDownPairs(mapString, ",", "=");
    }

    public static List<Map<String, String>> breakDownMapList(String mapList) {
        return breakDownList(mapList, ";")
                .map(ConfigUtils::breakDownMap)
                .collect(toList());
    }

    private static Map<String, String> breakDownPairs(String itemLine, String itemSplitter, String pairSplitter) {
        return breakDownPairs(itemLine, itemSplitter, pairSplitter, toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, List<String>> breakDownMultiValuePairs(String itemLine, String itemSplitter, String pairSplitter) {
        return breakDownPairs(itemLine, itemSplitter, pairSplitter, groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toList())));
    }

    private static <T> Map<String, T> breakDownPairs(String itemList, String itemSplitter, String pairSplitter, Collector<Map.Entry<String, String>, ?, Map<String, T>> collector) {
        return breakDownList(itemList, itemSplitter)
                .map(headerLine -> breakDownPair(headerLine, pairSplitter))
                .collect(collector);
    }

    private static Map.Entry<String, String> breakDownPair(String pairLine, String pairSplitter) {
        String[] parts = pairLine.split(pairSplitter, 2);
        if (parts.length < 2) {
            throw new IllegalStateException("Incomplete pair: " + pairLine);
        }
        return new AbstractMap.SimpleEntry<>(parts[0].trim(), parts[1].trim());
    }

    public static List<String> breakDownList(String itemList) {
        return breakDownList(itemList, ",")
                .collect(toList());
    }

    private static Stream<String> breakDownList(String itemList, String splitter) {
        if (itemList==null || itemList.length()==0) {
            return Stream.empty();
        }
        return Stream.of(itemList.split(splitter))
                .map(String::trim)
                .filter(it -> !it.isEmpty());
    }

    public static Set<Integer> parseIntegerRangedList(String rangedList) {
        return breakDownList(rangedList, ",")
                .map(ConfigUtils::parseIntegerRange)
                .flatMap(Set::stream)
                .collect(toSet());
    }

    private static Set<Integer> parseIntegerRange(String range) {
        String[] rangeString = range.split("\\.\\.");
        if (rangeString.length==0 || rangeString[0].length()==0) {
            return emptySet();
        } else if (rangeString.length==1) {
            return asSet(Integer.valueOf(rangeString[0].trim()));
        } else if (rangeString.length==2) {
            int from = Integer.parseInt(rangeString[0].trim());
            int to = Integer.parseInt(rangeString[1].trim());
            return (from < to ? rangeClosed(from, to):rangeClosed(to, from)).boxed().collect(toSet());
        }
        throw new IllegalStateException(String.format("Invalid range definition %s", range));
    }

    private static Set<Integer> asSet(Integer... values) {
        return Stream.of(values).collect(toSet());
    }
}
