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

package io.confluent.connect.jdbc.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class TimestampIncrementingOffset extends HashMap<String, Object> {
    private static final Logger log = LoggerFactory.getLogger(TimestampIncrementingOffset.class);
    static final String INCREMENTING_FIELD = "incrementing";
    static final String TIMESTAMP_FIELD = "timestamp";
    static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

    /**
     * @param timestampOffset    the timestamp offset.
     *                           If null, {@link #getTimestampOffset()} will return
     *                           {@code new Timestamp(0)}.
     * @param incrementingOffset the incrementing offset.
     *                           If null, {@link #getIncrementingOffset()} will return -1.
     */
    public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset) {
        super();
        setTimestampOffset(timestampOffset);
        setIncrementingOffset(incrementingOffset);
    }

    public TimestampIncrementingOffset(Map<? extends String, ?> m) {
        super(m);
    }

    public long getIncrementingOffset() {
        Long incr = (Long) get(INCREMENTING_FIELD);
        return incr == null ? -1 : incr;
    }

    public void setIncrementingOffset(Long incrementingOffset) {
        put(INCREMENTING_FIELD, incrementingOffset);
    }

    public Timestamp getTimestampOffset() {
        Long millis = (Long) get(TIMESTAMP_FIELD);
        Timestamp ts = null;
        if (millis != null) {
            log.trace("millis is not null");
            ts = new Timestamp(millis);
            Long nanos = (Long) get(TIMESTAMP_NANOS_FIELD);
            if (nanos != null) {
                log.trace("Nanos is not null");
                ts.setNanos(nanos.intValue());
            }
        }
        return ts;
    }

    public void setTimestampOffset(Timestamp timestampOffset) {
        put(TIMESTAMP_FIELD, timestampOffset == null ? null : timestampOffset.getTime());
        put(TIMESTAMP_NANOS_FIELD, timestampOffset == null ? null : timestampOffset.getNanos());
    }

    public boolean hasTimestampOffset() {
        return get(TIMESTAMP_FIELD) != null;
    }

    public Map<String, Object> toMap() {
        return this;
    }

    public static TimestampIncrementingOffset fromMap(Map<String, ?> map) {
        return new TimestampIncrementingOffset(map);
    }

}
