package com.github.fevernova.task.rdb;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractBatchSource;
import com.github.fevernova.io.rdb.ds.RDBDataSource;
import com.github.fevernova.io.rdb.schema.Column;
import com.github.fevernova.io.rdb.schema.Table;
import com.github.fevernova.task.rdb.data.ListData;
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
public class JobSource extends AbstractBatchSource<Long, ListData> implements RDBDataSource.ICallable<Boolean> {


    private static final String SQL_QUERY_TEMPLETE = "SELECT %s FROM %s WHERE %s > ? AND %s <= ? ";

    private static final String SQL_RANGE_TEMPLETE = "SELECT MIN(%s) , MAX(%s) FROM %s ";

    protected RDBDataSource dataSource;

    protected Table table;

    protected List<String> tableSeries;

    protected Integer tableSeriesIndex;

    protected int stepSize;

    protected boolean stepByTimeStamp;

    protected String sqlQuery;

    protected String sqlRange;

    protected long start;

    protected long end;

    protected long currentStart;

    protected long currentEnd;

    protected long totalCount;

    protected Integer keyColumnIndex;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        TaskContext dsContext = new TaskContext("datasource", super.taskContext.getSubProperties("datasource."));
        this.dataSource = RDBDataSource.createOf(dsContext);
        this.dataSource.initDataSource();
        String dbTableNames = taskContext.get("dbtables");
        Validate.notNull(dbTableNames);
        this.tableSeries = Util.splitStringWithFilter(dbTableNames, ",", null);
        this.tableSeriesIndex = super.index;
        String dbTableName = this.tableSeries.get(this.tableSeriesIndex);
        String dbName = dbTableName.split("\\.")[0];
        String tableName = dbTableName.split("\\.")[1];
        this.table = this.dataSource.config(dbName, tableName, taskContext.getString("sensitivecolumns"));
        this.stepByTimeStamp = taskContext.getBoolean("stepbytimestamp", false);
        this.stepSize = taskContext.getInteger("stepsize", this.stepByTimeStamp ? 60 * 1000 : 1000);
        init4Table(this.table.getDbTableName());
    }


    private void init4Table(String dbTableName) {

        log.info("locate at " + dbTableName);
        String rangeColumn = super.taskContext.getString("rangecolumn");
        if (rangeColumn == null) {
            rangeColumn = getPrimaryKey().escapeName();
        }
        List<String> columnsName = this.table.getColumnsWithoutIgnore().stream().map(column -> column.escapeName()).collect(Collectors.toList());
        this.sqlQuery = String.format(SQL_QUERY_TEMPLETE, StringUtils.join(columnsName, ","), dbTableName, rangeColumn, rangeColumn);
        String extraSql = super.taskContext.getString("extrasql", "");
        this.sqlQuery = this.sqlQuery + extraSql;
        this.sqlRange = String.format(SQL_RANGE_TEMPLETE, rangeColumn, rangeColumn, dbTableName);

        Pair<Long, Long> range = this.stepByTimeStamp ? this.dataSource.executeQuery(this.sqlRange, r -> {
            r.next();
            return Pair.of(r.getTimestamp(1).getTime(), r.getTimestamp(2).getTime());
        }) : this.dataSource.executeQuery(this.sqlRange, r -> {
            r.next();
            return Pair.of(r.getLong(1), r.getLong(2));
        });
        this.start = super.taskContext.getLong("start", range.getKey());
        this.end = super.taskContext.getLong("end", range.getValue());

        this.currentStart = this.start - 1;
        this.currentEnd = this.start + this.stepSize;
        if (this.currentEnd > this.end) {
            this.currentEnd = this.end;
        }
        String keyColumnStr = super.taskContext.getString("keycolumn");
        Column keyColumn = null;
        if (keyColumnStr == null) {
            keyColumn = getPrimaryKey();
        } else if (!"NULL".equals(keyColumnStr)) {
            keyColumn = this.table.getColumns().stream().filter(column -> keyColumnStr.equals(column.getName())).findFirst().get();
        }
        if (keyColumn != null && !keyColumn.isIgnore()) {
            this.keyColumnIndex = this.table.getColumnsWithoutIgnore().indexOf(keyColumn) + 1;
        }
    }


    private Column getPrimaryKey() {

        final List<Column> primaryKeys = this.table.getColumns().stream().filter(column -> column.isPrimaryKey()).collect(Collectors.toList());
        Validate.isTrue(primaryKeys.size() == 1);
        return primaryKeys.get(0);
    }


    @Override public void work() {

        this.dataSource.executeQuery(this.sqlQuery, this);
        this.currentStart = this.currentEnd;
        this.currentEnd += this.stepSize;
        if (this.currentEnd > this.end) {
            this.currentEnd = this.end;
        }
        if (this.currentStart == this.end) {
            if (this.tableSeriesIndex + super.total >= this.tableSeries.size()) {
                super.jobFinished();
            } else {
                this.tableSeriesIndex += super.total;
                init4Table(this.tableSeries.get(this.tableSeriesIndex));
            }
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
            Long key = this.keyColumnIndex == null ? 0L : r.getLong(this.keyColumnIndex);
            ListData listData = feedOne(key);
            int i = 1;
            for (Column column : this.table.getColumnsWithoutIgnore()) {
                listData.getValues().add(Pair.of(column.getName(), r.getObject(i++)));
            }
            listData.setKey(key);
            this.totalCount++;
            push();
        }
        return true;
    }


    @Override public void jobFinishedListener() {

        log.info("job_history : {} , {} , {} , {} .", this.table.getDbTableName(), this.totalCount, super.startTime, super.endTime);
    }
}
