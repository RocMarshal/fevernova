package com.github.fevernova.task.mysql;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractBatchSink;
import com.github.fevernova.io.mysql.MysqlDataSource;
import com.github.fevernova.io.mysql.schema.Table;
import com.github.fevernova.task.mysql.data.ListData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class JobSink extends AbstractBatchSink {


    private static final String SQL_INSERT_TEMPLETE = "%s INTO %s ( %s ) VALUES ( %s )";

    protected MysqlDataSource dataSource;

    protected Table table;

    protected boolean truncate;

    protected int columnsNum;

    protected String sqlInsert;

    protected Connection connection;

    protected PreparedStatement preparedStatement;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        super.rollingSize = super.taskContext.getLong("rollingsize", 1000L);
        super.rollingPeriod = 1000L;
        super.lastRollingSeq = (Util.nowMS() / super.rollingPeriod);

        this.dataSource = new MysqlDataSource(new TaskContext("mysql", super.taskContext.getSubProperties("mysql.")));
        this.dataSource.initJDBC(false);
        String dbName = taskContext.getString("db");
        String tableName = taskContext.getString("table");
        this.dataSource.config(dbName, tableName, taskContext.getString("sensitivecolumns"));
        this.table = this.dataSource.getTable(dbName, tableName, true);
        this.truncate = taskContext.getBoolean("truncate", false);

        String mode = taskContext.getString("mode", "INSERT");//INSERT/REPLACE/INSERT IGNORE
        final List<String> columnsName = this.table.getColumns().stream().
                filter(column -> !column.isIgnore()).map(column -> column.escapeName()).collect(Collectors.toList());
        final List<String> paramsArray = this.table.getColumns().stream().
                filter(column -> !column.isIgnore()).map(column -> "?").collect(Collectors.toList());

        this.columnsNum = columnsName.size();
        this.sqlInsert = String.format(SQL_INSERT_TEMPLETE, mode, this.table.getDbTableName(),
                                       StringUtils.join(columnsName, ","), StringUtils.join(paramsArray, ","));
    }


    @Override public void onStart() {

        super.onStart();
        if (isFirst() && this.truncate) {
            this.dataSource.executeQuery("truncate table " + this.table.getDbTableName());
        }
    }


    @Override protected void batchPrepare(Data event) {

        try {
            this.connection = this.dataSource.getDataSource().getConnection();
            this.connection.setAutoCommit(false);
            this.preparedStatement = this.connection.prepareStatement(this.sqlInsert);
        } catch (Exception e) {
            log.error("batchPrepare error : ", e);
            Validate.isTrue(false);
        }
    }


    @Override protected int batchHandleEvent(Data dataEvent) {

        try {
            ListData listData = (ListData) dataEvent;
            for (int i = 0; i < this.columnsNum; i++) {
                this.preparedStatement.setObject(i + 1, listData.getValues().get(i).getValue());
            }
            this.preparedStatement.addBatch();
        } catch (Exception e) {
            log.error("batchHandleEvent error : ", e);
            Validate.isTrue(false);
        }
        return 1;
    }


    @Override protected void batchSync() throws IOException {

    }


    @Override protected void batchClose() throws Exception {

        this.preparedStatement.executeBatch();
        this.connection.commit();
        this.preparedStatement.close();
        this.connection.close();
        this.connection = null;
        this.preparedStatement = null;
    }


    @Override protected void batchWhenBarrierSnaptshot(BarrierData barrierData) {

    }
}
