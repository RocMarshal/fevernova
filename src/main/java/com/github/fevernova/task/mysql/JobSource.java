package com.github.fevernova.task.mysql;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.ComponentStatus;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.service.barrier.listener.BarrierCompletedListener;
import com.github.fevernova.io.mysql.MysqlDataSource;
import com.github.fevernova.io.mysql.schema.Column;
import com.github.fevernova.io.mysql.schema.Table;
import com.github.fevernova.task.mysql.data.ListData;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;


@Slf4j
public class JobSource extends AbstractSource<Long, ListData> implements MysqlDataSource.ICallable<Boolean>, BarrierCompletedListener {


    private static final String SQL_QUERY_TEMPLETE = "SELECT %s FROM %s WHERE %s > ? AND %s <= ? ";

    private static final String SQL_RANGE_TEMPLETE = "SELECT MIN(%s) , MAX(%s) FROM %s ";

    protected TaskContext dataSourceContext;

    protected MysqlDataSource dataSource;

    protected int stepSize;

    protected boolean stepByTimeStamp;

    protected String dbName;

    protected String tableName;

    protected String dbTableName;

    protected Table table;

    protected int columnsNum;

    protected String sqlQuery;

    protected String sqlRange;

    protected long start;

    protected long end;

    protected long currentStart;

    protected long currentEnd;

    protected Long lastBarrierId;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.dataSourceContext = new TaskContext("mysql", super.taskContext.getSubProperties("mysql."));
        this.dataSource = new MysqlDataSource(this.dataSourceContext);
        try {
            this.dataSource.initJDBC(false);
        } catch (Exception e) {
            log.error("source init error : ", e);
            Validate.isTrue(false);
        }
        this.stepByTimeStamp = taskContext.getBoolean("stepbytimestamp", false);
        this.stepSize = taskContext.getInteger("stepsize", this.stepByTimeStamp ? 60 * 1000 : 1000);
        this.dbName = taskContext.getString("db");
        this.tableName = taskContext.getString("table");
        this.dbTableName = this.dbName + "." + this.tableName;
        this.dataSource.config(this.dbName, this.tableName, taskContext.getString("sensitivecolumns"));
        this.table = this.dataSource.getTable(this.dbTableName, true);

        final List<String> columnsName = Lists.newArrayList();
        final List<String> primaryKeys = Lists.newArrayList();
        this.table.getColumns().forEach(column -> {

            if (!column.isIgnore()) {
                columnsName.add("`" + column.getName() + "`");
            }
            if (column.isPrimaryKey()) {
                primaryKeys.add("`" + column.getName() + "`");
            }
        });
        this.columnsNum = columnsName.size();
        String columnsNameString = StringUtils.join(columnsName, ",");
        String primaryColumnNameString = taskContext.getString("primarykey");
        if (primaryColumnNameString == null) {
            Validate.isTrue(primaryKeys.size() == 1);
            primaryColumnNameString = primaryKeys.get(0);
        }
        this.sqlQuery = String.format(SQL_QUERY_TEMPLETE, columnsNameString, this.dbTableName, primaryColumnNameString, primaryColumnNameString);
        this.sqlRange = String.format(SQL_RANGE_TEMPLETE, primaryColumnNameString, primaryColumnNameString, this.dbTableName);

        Pair<Long, Long> ps = this.stepByTimeStamp ? this.dataSource.executeQuery(this.sqlRange, r -> {
            r.next();
            return Pair.of(r.getTimestamp(1).getTime(), r.getTimestamp(2).getTime());
        }) : this.dataSource.executeQuery(this.sqlRange, r -> {
            r.next();
            return Pair.of(r.getLong(1), r.getLong(2));
        });
        this.start = taskContext.getLong("start", ps.getKey() - 1);
        this.end = taskContext.getLong("end", ps.getValue());
    }


    @Override public void onStart() {

        super.onStart();
        this.currentStart = this.start;
        this.currentEnd = this.currentStart + this.stepSize;
        if (this.currentEnd > this.end) {
            this.currentEnd = this.end;
        }
    }


    @Override public void work() {

        Validate.isTrue(this.dataSource.executeQuery(this.sqlQuery, this));
        this.currentStart = this.currentEnd;
        this.currentEnd += this.stepSize;
        if (this.currentEnd > this.end) {
            this.currentEnd = this.end;
        }
        if (this.currentStart == this.end) {
            super.onPause();
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
            ListData listData = feedOne(0L);
            int i = 1;
            for (Column column : this.table.getColumns()) {
                if (column.isIgnore()) {
                    continue;
                }
                listData.getValues().add(r.getObject(i++));
            }
            push();
        }
        return true;
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

        if (super.status == ComponentStatus.PAUSE) {
            this.lastBarrierId = barrierData.getBarrierId();
        }
    }


    @Override public void completed(BarrierData barrierData) throws Exception {

        if (this.lastBarrierId != null && this.lastBarrierId <= barrierData.getBarrierId()) {
            super.globalContext.fatalError("Job Finished . ", null);
        }
    }
}
