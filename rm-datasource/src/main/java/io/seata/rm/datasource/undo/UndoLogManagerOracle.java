/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.undo;

import java.io.ByteArrayInputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.util.JdbcConstants;
import io.seata.common.Constants;
import io.seata.common.exception.NotSupportYetException;
import io.seata.common.util.BlobUtils;
import io.seata.common.util.CollectionUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.exception.TransactionException;
import io.seata.rm.datasource.ConnectionContext;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.rm.datasource.sql.struct.TableMeta;

import io.seata.rm.datasource.sql.struct.TableMetaCacheOracle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.core.exception.TransactionExceptionCode.BranchRollbackFailed_Retriable;

/**
 * The type Undo log manager.
 * undo log管理器
 * @author ccg
 * @date 2019/3/25
 */
public final class UndoLogManagerOracle {

    private enum State {
        /**
         * This state can be properly rolled back by services
         * 能被service正确回滚
         */
        Normal(0),
        /**
         * This state prevents the branch transaction from inserting undo_log after the global transaction is rolled
         * back.
         * 防止全局事务回滚后，分支事务插入undo_log
         */
        GlobalFinished(1);

        private int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(UndoLogManagerOracle.class);

    // undo_log
    private static final String UNDO_LOG_TABLE_NAME = ConfigurationFactory.getInstance()
            .getConfig(ConfigurationKeys.TRANSACTION_UNDO_LOG_TABLE, ConfigurationKeys.TRANSACTION_UNDO_LOG_DEFAULT_TABLE);
    private static final String INSERT_UNDO_LOG_SQL = "INSERT INTO " + UNDO_LOG_TABLE_NAME + "\n" +
        "\t(id, branch_id, xid, context, rollback_info, log_status, log_created, log_modified)\n" +
        "VALUES (UNDO_LOG_SEQ.nextval, ?, ?, ?, ?, ?, sysdate, sysdate)";

    private static final String DELETE_UNDO_LOG_SQL = "DELETE FROM " + UNDO_LOG_TABLE_NAME + "\n" +
        "\tWHERE branch_id = ? AND xid = ?";

    private static final String SELECT_UNDO_LOG_SQL = "SELECT * FROM " + UNDO_LOG_TABLE_NAME
        + " WHERE  branch_id = ? AND xid = ? FOR UPDATE";

    private static final ThreadLocal<String> SERIALIZER_LOCAL = new ThreadLocal<>();

    private UndoLogManagerOracle() {

    }

    /**
     * Flush undo logs.
     * 刷新undo log到数据库
     *
     * @param cp the cp
     * @throws SQLException the sql exception
     */
    public static void flushUndoLogs(ConnectionProxy cp) throws SQLException {
        assertDbSupport(cp.getDbType());

        ConnectionContext connectionContext = cp.getContext();
        String xid = connectionContext.getXid();
        long branchID = connectionContext.getBranchId();

        BranchUndoLog branchUndoLog = new BranchUndoLog();
        branchUndoLog.setXid(xid);
        branchUndoLog.setBranchId(branchID);
        branchUndoLog.setSqlUndoLogs(connectionContext.getUndoItems());

        UndoLogParser parser = UndoLogParserFactory.getInstance();
        byte[] undoLogContent = parser.encode(branchUndoLog);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Flushing UNDO LOG: {}",new String(undoLogContent, Constants.DEFAULT_CHARSET));
        }

        insertUndoLogWithNormal(xid, branchID, buildContext(parser.getName()), undoLogContent, cp.getTargetConnection());
    }

    private static void assertDbSupport(String dbType) {
        if (!JdbcConstants.ORACLE.equalsIgnoreCase(dbType)) {
            throw new NotSupportYetException("DbType[" + dbType + "] is not support yet!");
        }
    }

    /**
     * Undo.
     * undo 操作
     *
     * @param dataSourceProxy the data source proxy
     * @param xid             the xid
     * @param branchId        the branch id
     * @throws TransactionException the transaction exception
     */
    public static void undo(DataSourceProxy dataSourceProxy, String xid, long branchId) throws TransactionException {
        assertDbSupport(dataSourceProxy.getDbType());

        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement selectPST = null;

        for (; ; ) {
            try {
                conn = dataSourceProxy.getPlainConnection();

                // The entire undo process should run in a local transaction.
                conn.setAutoCommit(false);

                // Find UNDO LOG
                // 找到undo log
                selectPST = conn.prepareStatement(SELECT_UNDO_LOG_SQL);
                selectPST.setLong(1, branchId);
                selectPST.setString(2, xid);
                rs = selectPST.executeQuery();
                boolean exists = false;
                while (rs.next()) {
                    exists = true;
                    // It is possible that the server repeatedly sends a rollback request to roll back
                    // the same branch transaction to multiple processes,
                    // ensuring that only the undo_log in the normal state is processed.
                    int state = rs.getInt("log_status");
                    if (!canUndo(state)) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("xid {} branch {}, ignore {} undo_log",
                                xid, branchId, state);
                        }
                        return;
                    }

                    String contextString = rs.getString("context");
                    Map<String, String> context = parseContext(contextString);

                    // 回滚内容
                    Blob b = rs.getBlob("rollback_info");
                    byte[] rollbackInfo = BlobUtils.blob2Bytes(b);

                    // 获取序列化器
                    String serializer = context == null ? null : context.get(UndoLogConstants.SERIALIZER_KEY);
                    UndoLogParser parser = serializer == null ? UndoLogParserFactory.getInstance() :
                        UndoLogParserFactory.getInstance(serializer);
                    BranchUndoLog branchUndoLog = parser.decode(rollbackInfo);

                    try {
                        // put serializer name to local
                        SERIALIZER_LOCAL.set(parser.getName());

                        for (SQLUndoLog sqlUndoLog : branchUndoLog.getSqlUndoLogs()) {
                            TableMeta tableMeta = TableMetaCacheOracle.getTableMeta(dataSourceProxy, sqlUndoLog.getTableName());
                            sqlUndoLog.setTableMeta(tableMeta); // TableRecords的tableMeta属性是transient
                            AbstractUndoExecutor undoExecutor = UndoExecutorFactory.getUndoExecutor(dataSourceProxy.getDbType(),
                                    sqlUndoLog);
                            undoExecutor.executeOn(conn);
                        }
                    } finally {
                        // remove serializer name
                        SERIALIZER_LOCAL.remove();
                    }
                }
                // If undo_log exists, it means that the branch transaction has completed the first phase,
                // we can directly roll back and clean the undo_log
                // Otherwise, it indicates that there is an exception in the branch transaction,
                // causing undo_log not to be written to the database.
                // For example, the business processing timeout, the global transaction is the initiator rolls back.
                // To ensure data consistency, we can insert an undo_log with GlobalFinished state
                // to prevent the local transaction of the first phase of other programs from being correctly submitted.
                // See https://github.com/seata/seata/issues/489
                // 如果undo_log存在，说明分支事务已经完成第一阶段，能直接回滚并且删除undo_log
                // 否则说明分支事务中存在异常导致undo_log没有写入数据库。
                // 比如，业务处理超时，全局事务指示回滚。为了保证数据一致性，我们使用GlobalFinished状态插入undo_log，防止一阶段的本地事务
                // 被正确提交

                if (exists) {
                    // 删除undo_log
                    deleteUndoLog(xid, branchId, conn);
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log deleted with {}",
                                xid, branchId, State.GlobalFinished.name());
                    }
                } else {
                    // undo_log不存在
                    insertUndoLogWithGlobalFinished(xid, branchId, UndoLogParserFactory.getInstance(), conn);
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log added with {}",
                                xid, branchId, State.GlobalFinished.name());
                    }
                }

                return;
            } catch (SQLIntegrityConstraintViolationException e) {
                // Possible undo_log has been inserted into the database by other processes, retrying rollback undo_log
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("xid {} branch {}, undo_log inserted, retry rollback",
                            xid, branchId);
                }
            } catch (Throwable e) {
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException rollbackEx) {
                        LOGGER.warn("Failed to close JDBC resource while undo ... ", rollbackEx);
                    }
                }
                throw new TransactionException(BranchRollbackFailed_Retriable, String.format("%s/%s", branchId, xid), e);

            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (selectPST != null) {
                        selectPST.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException closeEx) {
                    LOGGER.warn("Failed to close JDBC resource while undo ... ", closeEx);
                }
            }
        }
    }


    /**
     * batch Delete undo log.
     * 批量删除undo 日志
     *
     * @param xids
     * @param branchIds
     * @param conn
     */
    public static void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds, Connection conn) throws SQLException {
        if (CollectionUtils.isEmpty(xids) || CollectionUtils.isEmpty(branchIds)) {
            return;
        }
        int xidSize = xids.size();
        int branchIdSize = branchIds.size();
        String batchDeleteSql = toBatchDeleteUndoLogSql(xidSize, branchIdSize);
        PreparedStatement deletePST = null;
        try {
            deletePST = conn.prepareStatement(batchDeleteSql);
            int paramsIndex = 1;
            for (Long branchId : branchIds) {
                deletePST.setLong(paramsIndex++,branchId);
            }
            for (String xid: xids){
                deletePST.setString(paramsIndex++, xid);
            }
            int deleteRows = deletePST.executeUpdate();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch delete undo log size " + deleteRows);
            }
        }catch (Exception e){
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        } finally {
            if (deletePST != null) {
                deletePST.close();
            }
        }

    }

    protected static String toBatchDeleteUndoLogSql(int xidSize, int branchIdSize) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM ")
                .append(UNDO_LOG_TABLE_NAME)
                .append(" WHERE  branch_id IN ");
        appendInParam(branchIdSize, sqlBuilder);
        sqlBuilder.append(" AND xid IN ");
        appendInParam(xidSize, sqlBuilder);
        return sqlBuilder.toString();
    }

    protected static void appendInParam(int size, StringBuilder sqlBuilder) {
        sqlBuilder.append(" (");
        for (int i = 0;i < size;i++) {
            sqlBuilder.append("?");
            if (i < (size - 1)) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(") ");
    }

    /**
     * Delete undo log.
     * 删除undo_log
     *
     * @param xid the xid
     * @param branchId the branch id
     * @param conn the conn
     * @throws SQLException the sql exception
     */
    public static void deleteUndoLog(String xid, long branchId, Connection conn) throws SQLException {
        PreparedStatement deletePST = null;
        try {
            deletePST = conn.prepareStatement(DELETE_UNDO_LOG_SQL);
            deletePST.setLong(1, branchId);
            deletePST.setString(2, xid);
            deletePST.executeUpdate();
        }catch (Exception e){
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        } finally {
            if (deletePST != null) {
                deletePST.close();
            }
        }
    }
    public static String getCurrentSerializer() {
        return SERIALIZER_LOCAL.get();
    }

    // 插入Normal状态的undo log
    private static void insertUndoLogWithNormal(String xid, long branchID, String rollbackCtx,
                                                byte[] undoLogContent, Connection conn) throws SQLException {
        insertUndoLog(xid, branchID, rollbackCtx, undoLogContent, State.Normal, conn);
    }

    // 插入GlobalFinished状态的undo log
    private static void insertUndoLogWithGlobalFinished(String xid, long branchID, UndoLogParser parser,
        Connection conn) throws SQLException {
        insertUndoLog(xid, branchID, buildContext(parser.getName()),
            parser.getDefaultContent(), State.GlobalFinished, conn);
    }

    // 插入undo log
    private static void insertUndoLog(String xid, long branchID, String rollbackCtx,
        byte[] undoLogContent, State state, Connection conn) throws SQLException {
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(INSERT_UNDO_LOG_SQL);
            pst.setLong(1, branchID);
            pst.setString(2, xid);
            pst.setString(3, rollbackCtx);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(undoLogContent);
            pst.setBlob(4, inputStream);
            pst.setInt(5, state.getValue());
            pst.executeUpdate();
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        } finally {
            if (pst != null) {
                pst.close();
            }
        }
    }

    private static boolean canUndo(int state) {
        return state == State.Normal.getValue();
    }

    private static String buildContext(String serializer) {
        Map<String, String> map = new HashMap<>();
        map.put(UndoLogConstants.SERIALIZER_KEY, serializer);
        return CollectionUtils.encodeMap(map);
    }

    private static Map<String, String> parseContext(String data) {
        return CollectionUtils.decodeMap(data);
    }

}
