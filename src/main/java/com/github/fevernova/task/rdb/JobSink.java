package com.github.fevernova.task.rdb;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractBatchSink;
import com.github.fevernova.io.rdb.ds.RDBDataSource;
import com.github.fevernova.io.rdb.schema.Table;
import com.github.fevernova.task.rdb.data.ListData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class JobSink extends AbstractBatchSink {


    private static final String SQL_INSERT_TEMPLETE = "%s INTO %s ( %s ) VALUES ( %s ) ";

    protected RDBDataSource dataSource;

    protected Table table;

    protected String baseTableName;

    protected int columnsNum;

    protected String sqlInsert;

    protected Connection connection;

    protected PreparedStatement preparedStatement;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        super.rollingSize = taskContext.getLong("rollingsize", 1000L);
        super.rollingPeriod = 1000L;
        super.lastRollingSeq = (Util.nowMS() / super.rollingPeriod);

        TaskContext dsContext = new TaskContext("datasource", taskContext.getSubProperties("datasource."));
        this.dataSource = RDBDataSource.createOf(dsContext);
        this.dataSource.initDataSource();
        String dbName = taskContext.getString("db");
        String tableName = taskContext.getString("table");
        String dateSuffix = taskContext.getString("tabledatesuffix");
        if (StringUtils.isNotBlank(dateSuffix)) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateSuffix);
            this.baseTableName = tableName;
            tableName = tableName + simpleDateFormat.format(new Date());
        }
        boolean createTable = taskContext.getBoolean("createtable", false);
        if (isFirst() && createTable) {
            this.dataSource.executeQuery(String.format(this.dataSource.getCreateTableTemplete(), dbName, tableName, dbName, this.baseTableName));
        }
        this.table = this.dataSource.config(dbName, tableName, taskContext.getString("sensitivecolumns"));
        boolean truncate = taskContext.getBoolean("truncate", false);
        if (isFirst() && truncate) {
            this.dataSource.executeQuery("truncate table " + this.table.getDbTableName());
        }
        String mode = taskContext.getString("mode", "INSERT");//INSERT/REPLACE/INSERT IGNORE
        final List<String> columnsName =
                this.table.getColumnsWithoutIgnore().stream().map(column -> column.escapeName()).collect(Collectors.toList());
        final List<String> paramsArray = columnsName.stream().map(column -> "?").collect(Collectors.toList());
        this.columnsNum = columnsName.size();
        this.sqlInsert = String.format(SQL_INSERT_TEMPLETE, mode, this.table.getDbTableName(),
                                       StringUtils.join(columnsName, ","), StringUtils.join(paramsArray, ","));
        this.sqlInsert = this.sqlInsert + this.dataSource.processExtraSql4Upsert(this.table, taskContext.getString("extrasql", ""));
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


    @Override protected void batchClose(boolean bySnapshot) throws Exception {

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
