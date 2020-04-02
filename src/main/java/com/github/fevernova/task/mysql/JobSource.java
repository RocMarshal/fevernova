package com.github.fevernova.task.mysql;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractBatchSource;
import com.github.fevernova.io.mysql.MysqlDataSource;
import com.github.fevernova.io.mysql.schema.Column;
import com.github.fevernova.io.mysql.schema.Table;
import com.github.fevernova.task.mysql.data.ListData;
import com.github.fevernova.task.mysql.data.MysqlJDBCType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class JobSource extends AbstractBatchSource<Integer, ListData> implements MysqlDataSource.ICallable<Boolean> {


    private static final String SQL_QUERY_TEMPLETE = "SELECT %s FROM %s WHERE %s > ? AND %s <= ? ";

    private static final String SQL_RANGE_TEMPLETE = "SELECT MIN(%s) , MAX(%s) FROM %s ";

    protected MysqlDataSource dataSource;

    protected Table table;

    protected int stepSize;

    protected boolean stepByTimeStamp;

    protected String sqlQuery;

    protected String sqlRange;

    protected long start;

    protected long end;

    protected long currentStart;

    protected long currentEnd;

    protected long totalCount;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.dataSource = new MysqlDataSource(new TaskContext("mysql", super.taskContext.getSubProperties("mysql.")));
        this.dataSource.init(new MysqlJDBCType(), false);
        String dbName = taskContext.getString("db");
        String tableName = taskContext.getString("table");
        this.dataSource.config(dbName, tableName, taskContext.getString("sensitivecolumns"));
        this.table = this.dataSource.getTable(dbName, tableName, true);

        this.stepByTimeStamp = taskContext.getBoolean("stepbytimestamp", false);
        this.stepSize = taskContext.getInteger("stepsize", this.stepByTimeStamp ? 60 * 1000 : 1000);

        final List<String> columnsName = this.table.getColumns().stream().
                filter(column -> !column.isIgnore()).map(column -> column.escapeName()).collect(Collectors.toList());
        final List<String> primaryKeys = this.table.getColumns().stream().
                filter(column -> column.isPrimaryKey()).map(column -> column.escapeName()).collect(Collectors.toList());

        String primaryKey = taskContext.getString("primarykey");
        if (primaryKey == null) {
            Validate.isTrue(primaryKeys.size() == 1);
            primaryKey = primaryKeys.get(0);
        }
        this.sqlQuery = String.format(SQL_QUERY_TEMPLETE, StringUtils.join(columnsName, ","), this.table.getDbTableName(), primaryKey, primaryKey);
        String extraSql = taskContext.getString("extrasql", "");
        this.sqlQuery = this.sqlQuery + extraSql;
        this.sqlRange = String.format(SQL_RANGE_TEMPLETE, primaryKey, primaryKey, this.table.getDbTableName());

        Pair<Long, Long> range = this.stepByTimeStamp ? this.dataSource.executeQuery(this.sqlRange, r -> {
            r.next();
            return Pair.of(r.getTimestamp(1).getTime(), r.getTimestamp(2).getTime());
        }) : this.dataSource.executeQuery(this.sqlRange, r -> {
            r.next();
            return Pair.of(r.getLong(1), r.getLong(2));
        });
        this.start = taskContext.getLong("start", range.getKey());
        this.end = taskContext.getLong("end", range.getValue());
    }


    @Override public void onStart() {

        super.onStart();
        this.currentStart = this.start - 1;
        this.currentEnd = this.start + this.stepSize;
        if (this.currentEnd > this.end) {
            this.currentEnd = this.end;
        }
    }


    @Override public void work() {

        this.dataSource.executeQuery(this.sqlQuery, this);
        this.currentStart = this.currentEnd;
        this.currentEnd += this.stepSize;
        if (this.currentEnd > this.end) {
            this.currentEnd = this.end;
        }
        if (this.currentStart == this.end) {
            super.jobFinished();
        }
    }


    @Override public void handleParams(PreparedStatement p) throws Exception {

        if (this.stepByTimeStamp) {
            p.setTimestamp(1, new Timestamp(this.currentStart));
            p.setTimestamp(2, new Timestamp(this.currentEnd));
        } else {
            p.setLong(1, this.currentStart);
            p.setLong(2, this.currentEnd);
        }
    }


    @Override public Boolean handleResultSet(ResultSet r) throws Exception {

        while (r.next()) {
            ListData listData = feedOne(0);
            int i = 1;
            for (Column column : this.table.getColumns()) {
                if (!column.isIgnore()) {
                    listData.getValues().add(Pair.of(column.getName(), r.getObject(i++)));
                }
            }
            this.totalCount++;
            push();
        }
        return true;
    }


    @Override public void jobFinishedListener() {

        log.info("job_history : {} , {} , {} , {} .", this.table.getDbTableName(), this.totalCount, this.startTime, this.endTime);
    }
}
