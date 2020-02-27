package com.github.fevernova.task.mysql;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractBatchSink;
import com.github.fevernova.io.mysql.MysqlDataSource;
import com.github.fevernova.io.mysql.schema.Table;
import com.github.fevernova.task.mysql.data.ListData;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;


@Slf4j
public class JobSink extends AbstractBatchSink {


    private static final String SQL_INSERT_TEMPLETE = "%s INTO %s ( %s ) VALUES ( %s )";

    protected TaskContext dataSourceContext;

    protected MysqlDataSource dataSource;

    protected String dbName;

    protected String tableName;

    protected String dbTableName;

    protected Table table;

    protected int columnsNum;

    protected String sqlInsert;

    protected Connection connection;

    protected PreparedStatement preparedStatement;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.dataSourceContext = new TaskContext("mysql", super.taskContext.getSubProperties("mysql."));
        this.dataSource = new MysqlDataSource(this.dataSourceContext);
        try {
            this.dataSource.initJDBC(false);
        } catch (Exception e) {
            log.error("source init error : ", e);
            Validate.isTrue(false);
        }
        this.dbName = taskContext.getString("db");
        this.tableName = taskContext.getString("table");
        this.dbTableName = this.dbName + "." + this.tableName;
        this.dataSource.config(this.dbName, this.tableName, taskContext.getString("sensitivecolumns"));
        this.table = this.dataSource.getTable(this.dbTableName, true);

        String insertMode = taskContext.getString("insertmode", "INSERT");
        final List<String> columnsName = Lists.newArrayList();
        final List<String> paramsArray = Lists.newArrayList();
        this.table.getColumns().forEach(column -> {

            paramsArray.add("?");
            if (!column.isIgnore()) {
                columnsName.add("`" + column.getName() + "`");
            }
        });
        this.columnsNum = columnsName.size();
        this.sqlInsert = String.format(SQL_INSERT_TEMPLETE, insertMode, this.dbTableName,
                                       StringUtils.join(columnsName, ","), StringUtils.join(paramsArray, ","));
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
                this.preparedStatement.setObject(i + 1, listData.getValues().get(i) );
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
