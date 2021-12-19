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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.UPDATE_INSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final TableId tableId;
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private List<SinkRecord> records = new ArrayList<>();
    private List<SinkRecord> missingRecords = new ArrayList<>();
    private Schema keySchema;
    private Schema valueSchema;
    private RecordValidator recordValidator;
    private FieldsMetadata fieldsMetadata;
    private PreparedStatement upsertPreparedStatement;
    private PreparedStatement deletePreparedStatement;
    private PreparedStatement missingInsertPreparedStatement;
    private StatementBinder upsertStatementBinder;
    private StatementBinder deleteStatementBinder;
    private StatementBinder missingInsertStatementBinder;
    private boolean deletesInBatch = false;

    public BufferedRecords(
            JdbcSinkConfig config,
            TableId tableId,
            DatabaseDialect dbDialect,
            DbStructure dbStructure,
            Connection connection
    ) {
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
        this.recordValidator = RecordValidator.create(config);
    }

    public List<SinkRecord> add(SinkRecord record) throws SQLException {
        recordValidator.validate(record);
        final List<SinkRecord> flushed = new ArrayList<>();

        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.keySchema())) { //主键schema更新
            keySchema = record.keySchema();
            schemaChanged = true;
        }

        if (isNull(record.valueSchema())) { // 空记录需要删除
            // For deletes, value and optionally value schema come in as null.
            // We don't want to treat this as a schema change if key schemas is the same
            // otherwise we flush unnecessarily.
            if (config.deleteEnabled) {
                deletesInBatch = true;
            }
        } else if (Objects.equals(valueSchema, record.valueSchema())) { // 数据schema无变化，若有删除，则先提交执行删除
            if (config.deleteEnabled && deletesInBatch) {
                // flush so an insert after a delete of same record isn't lost
                flushed.addAll(flush());
            }
        } else { // 数据schema有变化
            // value schema is not null and has changed. This is a real schema change.
            valueSchema = record.valueSchema();
            schemaChanged = true;
        }

        // schema有变化，先提交更新之前的记录，然后准备好新的statement实例
        if (schemaChanged || upsertStatementBinder == null) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());

            // re-initialize everything that depends on the record schema
            final SchemaPair schemaPair = new SchemaPair(
                    record.keySchema(),
                    record.valueSchema()
            );
            fieldsMetadata = FieldsMetadata.extract(
                    tableId.tableName(),
                    config.pkMode,
                    config.pkFields,
                    config.fieldsWhitelist,
                    schemaPair
            );
            dbStructure.createOrAmendIfNecessary(
                    config,
                    connection,
                    tableId,
                    fieldsMetadata
            );

            // 清理
            close();

            // 构造
            Pair<PreparedStatement, StatementBinder> pair = createStatementAndBinder(config.insertMode, schemaPair);
            upsertPreparedStatement = pair.getLeft();
            upsertStatementBinder = pair.getRight();

            pair = createStatementAndBinder(INSERT, schemaPair);
            missingInsertPreparedStatement = pair.getLeft();
            missingInsertStatementBinder = pair.getRight();

            pair = createStatementAndBinder(null, schemaPair);
            if (config.deleteEnabled && nonNull(pair)) {
                deletePreparedStatement = pair.getLeft();
                deleteStatementBinder = pair.getRight();
            }
        }

        // set deletesInBatch if schema value is not null
        if (isNull(record.value()) && config.deleteEnabled) { // 标记有删除
            deletesInBatch = true;
        }

        records.add(record);

        // 缓存超出批次大小，则提交更新
        if (records.size() >= config.batchSize) {
            flushed.addAll(flush());
        }

        if (config.insertMode == UPDATE_INSERT && records.size() > 0){
            flushed.addAll(flush());
        }

        return flushed;
    }

    @SneakyThrows
    protected Pair<PreparedStatement, StatementBinder> createStatementAndBinder(JdbcSinkConfig.InsertMode mode, SchemaPair schemaPair) {
        String sql = mode == null ? getDeleteSql() : getUpsertSql(mode);
        log.debug("Create {} sql: {}", mode == null ? "DELETE" : mode, sql);
        if (StringUtils.isEmpty(sql)) return null;

        PreparedStatement statement = dbDialect.createPreparedStatement(connection, sql);
        StatementBinder binder = dbDialect.statementBinder(
                statement,
                config.pkMode,
                schemaPair,
                fieldsMetadata,
                dbStructure.tableDefinition(connection, tableId),
                mode == null ? config.insertMode : mode
        );

        return Pair.of(statement, binder);
    }

    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (SinkRecord record : records) {
            if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
                deleteStatementBinder.bindRecord(record);
            } else {
                upsertStatementBinder.bindRecord(record);
            }
        }

        Optional<Long> totalUpsertCount = executeUpdates();
        final long expectedCount = expectedUpdateCount();
        if (totalUpsertCount.filter(total -> total != expectedCount).isPresent()) {
            if (config.insertMode == INSERT) {
                throw new ConnectException(String.format(
                        "实际新增数：(%d)，小于当前接收记录数：(%d)",
                        totalUpsertCount.get(),
                        expectedCount
                ));
            } else if (config.insertMode == UPDATE_INSERT) {
                if (totalUpsertCount.get() > expectedCount) {
                    throw new ConnectException(String.format(
                            "实际更新数：(%d)，大于当前接收记录数：(%d)，请检查逻辑主键（pk-fields）设置",
                            totalUpsertCount.get(),
                            expectedCount
                    ));
                } else {
                    long totalInsertCount = executeMissingInsert();
                    totalUpsertCount = totalUpsertCount.map(count -> count + totalInsertCount);
                    if (totalUpsertCount.get() != expectedCount) {
                        throw new ConnectException(String.format(
                                "实际新增/更新数：(%d)，小于当前接收记录数：(%d)",
                                totalUpsertCount.get(),
                                expectedCount
                        ));
                    }
                }
            }
        }

        long totalDeleteCount = executeDeletes();
        log.info("{} records:{} resulting in totalUpsertCount:{} totalDeleteCount:{}",
                config.insertMode, records.size(), totalUpsertCount, totalDeleteCount
        );

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        missingRecords = new ArrayList<>();
        deletesInBatch = false;
        return flushedRecords;
    }

    /**
     * @return an optional count of all updated rows or an empty optional if no info is available
     */
    private Optional<Long> executeUpdates() throws SQLException {
        Optional<Long> count = Optional.empty();

        int[] updateCounts = upsertPreparedStatement.executeBatch();
        for (int i = 0; i < updateCounts.length; i++) {
            int updateCount = updateCounts[i];
            if (updateCount != Statement.SUCCESS_NO_INFO) {
                count = count.isPresent()
                        ? count.map(total -> total + updateCount)
                        : Optional.of((long) updateCount);
                if (updateCount == 0) {
                    missingRecords.add(records.get(i));
                }
            }
        }

        return count;
    }

    private long executeDeletes() throws SQLException {
        long totalDeleteCount = 0;
        if (nonNull(deletePreparedStatement)) {
            for (int updateCount : deletePreparedStatement.executeBatch()) {
                if (updateCount != Statement.SUCCESS_NO_INFO) {
                    totalDeleteCount += updateCount;
                }
            }
        }
        return totalDeleteCount;
    }

    private long executeMissingInsert() throws SQLException{
        long totalInsertCount = 0;

        for (SinkRecord record : missingRecords) {
            if (nonNull(record.value()) && nonNull(missingInsertPreparedStatement)) {
                missingInsertStatementBinder.bindRecord(record);
            }
        }

        int[] updateCounts = missingInsertPreparedStatement.executeBatch();
        for (int i = 0; i < updateCounts.length; i++) {
            int updateCount = updateCounts[i];
            if (updateCount != Statement.SUCCESS_NO_INFO) {
                totalInsertCount += updateCount;
            }
        }

        return totalInsertCount;
    }

    private long expectedUpdateCount() {
        return records
                .stream()
                // ignore deletes
                .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
                .count();
    }

    public void close() throws SQLException {
        log.debug(
                "Closing BufferedRecords with upsertPreparedStatement: {} deletePreparedStatement: {} " +
                        "insertPreparedStatement: {}",
                upsertPreparedStatement,
                deletePreparedStatement,
                missingInsertPreparedStatement
        );
        if (nonNull(upsertPreparedStatement)) {
            upsertPreparedStatement.close();
            upsertPreparedStatement = null;
        }
        upsertStatementBinder = null;
        if (nonNull(deletePreparedStatement)) {
            deletePreparedStatement.close();
            deletePreparedStatement = null;
        }
        deleteStatementBinder = null;
        if (nonNull(missingInsertPreparedStatement)) {
            missingInsertPreparedStatement.close();
            missingInsertPreparedStatement = null;
        }
        missingInsertStatementBinder = null;
    }

    private String getUpsertSql(JdbcSinkConfig.InsertMode mode) throws SQLException {
        switch (mode) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, tableId)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            tableId
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                            tableId,
                            asColumns(fieldsMetadata.keyFieldNames),
                            asColumns(fieldsMetadata.nonKeyFieldNames),
                            dbStructure.tableDefinition(connection, tableId)
                    );
                } catch (UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                            tableId,
                            dbDialect.name()
                    ));
                }
            case UPDATE:
            case UPDATE_INSERT:
                return dbDialect.buildUpdateStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, tableId)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private String getDeleteSql() {
        String sql = null;
        if (config.deleteEnabled) {
            switch (config.pkMode) {
                case RECORD_KEY:
                    if (fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }
                    try {
                        sql = dbDialect.buildDeleteStatement(
                                tableId,
                                asColumns(fieldsMetadata.keyFieldNames)
                        );
                    } catch (UnsupportedOperationException e) {
                        throw new ConnectException(String.format(
                                "Deletes to table '%s' are not supported with the %s dialect.",
                                tableId,
                                dbDialect.name()
                        ));
                    }
                    break;

                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    private Collection<ColumnId> asColumns(Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
    }
}
