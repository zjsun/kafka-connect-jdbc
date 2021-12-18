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

package io.confluent.connect.jdbc.sink;

import com.datav.scdf.kafka.common.ConfigUtils;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JdbcDbWriter {
    private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    final CachedConnectionProvider cachedConnectionProvider;

    public static final String DKE_OP_ID = ConfigUtils.DKE_OP_ID; // 时间戳字段
    private long latestOpId = 0; //最新时间戳（DKE_OP_ID字段值）
    private final Set<TableId> allTables = new HashSet<>(); // 当前实例处理过的所有表
    private final Set<String> allTopics = new HashSet<>(); // 当前实例处理过的所有topic
    private final SharedTopicAdmin topicAdmin;


    JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;

        this.cachedConnectionProvider = connectionProvider(
                config.connectionAttempts,
                config.connectionBackoffMs
        );

        this.topicAdmin = new SharedTopicAdmin(config.originals());
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                log.info("JdbcDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    void write(final Collection<SinkRecord> records)
            throws SQLException, TableAlterOrCreateException {
        final Connection connection = cachedConnectionProvider.getConnection();
        try {
            final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
            for (SinkRecord record : records) {
                final TableId tableId = destinationTable(record.topic());

                // 记录
                allTopics.add(record.topic());
                allTables.add(tableId);
                extractOpId(record);

                BufferedRecords buffer = bufferByTable.get(tableId);
                if (buffer == null) {
                    buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
                    bufferByTable.put(tableId, buffer);
                }
                buffer.add(record);
            }

            for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
                TableId tableId = entry.getKey();
                BufferedRecords buffer = entry.getValue();
                log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
                buffer.flush();
                buffer.close();
            }
            connection.commit();
        } catch (SQLException | TableAlterOrCreateException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                e.addSuppressed(sqle);
            } finally {
                throw e;
            }
        }
    }

    /**
     * 清理：1）数据topic；2）目标表中时间戳小于当前批次时间戳的记录（即源表已被物理删除的记录）
     */
    public void cleanup() {
        // 1) 删除topic
        if (!allTopics.isEmpty()) {
            log.info("Cleaning up topics: " + StringUtils.join(allTopics));
            this.topicAdmin.topicAdmin().admin().deleteTopics(allTopics);
            log.info("Topics deleted: " + StringUtils.join(allTopics));
        }

        // 2) 删除记录
        if (!allTables.isEmpty() && latestOpId > 0) {
            log.info("Cleaning up tables: " + StringUtils.join(allTables));
            final Connection connection = cachedConnectionProvider.getConnection();
            try {
                for (TableId tableId : allTables) {
                    String deleteSql = dbDialect.buildDeleteStatement(tableId, Collections.singleton(new ColumnId(tableId, DKE_OP_ID)));
                    deleteSql = StringUtils.replace(deleteSql, "= ?", "< ?");
                    try (PreparedStatement deleteStatement = dbDialect.createPreparedStatement(connection, deleteSql)) {
                        deleteStatement.setLong(1, latestOpId);
                        int deletedCount = deleteStatement.executeUpdate();
                        log.info("Deleting {} rows from {}", new Object[]{deletedCount, tableId});
                    }
                }

                connection.commit();
                log.info("Tables all cleaned: " + StringUtils.join(allTables));
            } catch (Exception e) {
                try {
                    connection.rollback();
                } catch (Exception ex) {
                    e.addSuppressed(ex);
                } finally {
                    throw new ConnectException(e.getMessage(), e);
                }
            }
        }
    }

    void extractOpId(SinkRecord record) {
        Struct struct = (Struct) record.value();
        Schema schema = record.valueSchema();
        if (struct != null && schema != null && schema.field(DKE_OP_ID) != null) {
            Long opId = struct.getInt64(DKE_OP_ID);
            if (opId != null && opId.longValue() > latestOpId) {
                latestOpId = opId.longValue();
            }
        }
    }

    void closeQuietly() {
        cachedConnectionProvider.close();
    }

    TableId destinationTable(String topic) {
        final String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (tableName.isEmpty()) {
            throw new ConnectException(String.format(
                    "Destination table name for topic '%s' is empty using the format string '%s'",
                    topic,
                    config.tableNameFormat
            ));
        }
        return dbDialect.parseTableIdentifier(tableName);
    }
}
