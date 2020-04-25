package com.github.fevernova.io.rdb.ds;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.data.TypeMatchRouter;
import com.github.fevernova.io.data.TypeRouter;
import com.github.fevernova.io.data.message.Meta;
import com.github.fevernova.io.rdb.schema.Column;
import com.github.fevernova.io.rdb.schema.Table;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Getter
public class BinlogDataSource extends MysqlDataSource {


    private long serverId;

    private long slaveId;

    private TypeMatchRouter typeMatchRouter;


    public BinlogDataSource(TaskContext context, TypeMatchRouter typeMatchRouter) {

        super(context);
        this.slaveId = context.getLong("slaveid", 65535L);
        this.typeMatchRouter = typeMatchRouter;
    }


    @Override public void initDataSource() {

        super.initDataSource();
        this.serverId = _getServerId();
        _checkVar("log_bin", "ON");
        _checkVar("binlog_format", "ROW");
        _checkVar("binlog_row_image", "FULL");
        //_checkVar("gtid_mode", "ON");
        //_checkVar("log_slave_updates", "ON");
        //_checkVar("enforce_gtid_consistency", "ON");
    }


    public void config(Set<String> whiteList, Map<String, String> mapping) {

        for (String item : whiteList) {
            String topic = mapping.get(item + ".topic");
            Set<String> ignoreColumnNames = Sets.newHashSet();
            String sensitiveColumns = mapping.get(item + ".sensitivecolumns");
            if (!StringUtils.isEmpty(sensitiveColumns)) {
                List<String> columnNames = Util.splitStringWithFilter(sensitiveColumns, "\\s|,", null);
                ignoreColumnNames.addAll(columnNames);
            }
            String dbName = item.split("\\.")[0];
            String tableName = item.split("\\.")[1];
            Table table = Table.builder().dbTableName(item).db(dbName).table(tableName).topic(topic).columns(Lists.newArrayList()).
                    ignoreColumnName(ignoreColumnNames).build();
            reloadSchema(table);
            super.schema.put(item, table);
        }
    }


    @Override protected void reloadSchema(Table table) {

        table.getColumns().clear();
        String sql = "SELECT COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE,CHARACTER_SET_NAME,"
                     + "NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,COLUMN_TYPE,COLUMN_KEY"
                     + " FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        executeQuery(sql, new ICallable<Object>() {


            @Override public void handleParams(PreparedStatement p) throws Exception {

                p.setString(1, table.getDb());
                p.setString(2, table.getTable());
            }


            @Override public Object handleResultSet(ResultSet r) throws Exception {

                while (r.next()) {
                    boolean ignore = table.getIgnoreColumnName().contains(r.getString("COLUMN_NAME"));
                    String charset = r.getString("CHARACTER_SET_NAME");
                    TypeRouter typeRouter = typeMatchRouter.convert(r.getString("DATA_TYPE"), charset);
                    table.getColumns().add(Column.builder().name(r.getString("COLUMN_NAME"))
                                                   .seq(r.getInt("ORDINAL_POSITION"))
                                                   .type(r.getString("DATA_TYPE"))
                                                   .primaryKey("PRI".equals(r.getString("COLUMN_KEY")))
                                                   .charset(charset)
                                                   .ignore(ignore)
                                                   .typeRouter(typeRouter)
                                                   .escapeLetter(getEscapeLetter())
                                                   .build());
                }
                return null;
            }
        });
        List<Meta.MetaEntity> entityList = Lists.newArrayList();
        table.getColumns().forEach(column -> entityList.add(new Meta.MetaEntity(column.getName(), column.getTypeRouter().getTargetType())));
        table.setMeta(new Meta(entityList));
    }


    private long _getServerId() {

        String sql = "show variables like 'server_id';";
        Long result = executeQuery(sql, r -> r.next() ? r.getLong(2) : null);
        return result;
    }


    private void _checkVar(String var, String expect) {

        String sql = "show variables like '" + var + "';";
        executeQuery(sql, (ICallable<Long>) r -> {

            if (!r.next()) {
                Validate.isTrue(false, var + " is null");
            }
            String s = r.getString("Value");
            Validate.isTrue(expect.equals(s));
            return null;
        });
    }
}
