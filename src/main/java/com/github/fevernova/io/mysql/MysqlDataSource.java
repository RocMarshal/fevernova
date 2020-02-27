package com.github.fevernova.io.mysql;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.data.TypeRouter;
import com.github.fevernova.io.data.message.Meta;
import com.github.fevernova.io.mysql.schema.Column;
import com.github.fevernova.io.mysql.schema.Table;
import com.github.fevernova.task.binlog.util.MysqlType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Getter
@Slf4j
public class MysqlDataSource {


    private TaskContext mysqlContext;

    private String host;

    private int port;

    private long serverId;

    private long slaveId;

    private String version;

    private String username;

    private String password;

    private String jdbcUrl;

    private DataSource dataSource;

    private Map<String, Table> schema = Maps.newConcurrentMap();


    public MysqlDataSource(TaskContext mysqlContext) {

        this.mysqlContext = mysqlContext;
        this.host = mysqlContext.getString("host", "127.0.0.1");
        this.port = mysqlContext.getInteger("port", 3306);
        this.slaveId = mysqlContext.getLong("slaveid", 65535L);
        this.username = mysqlContext.get("username");
        this.password = mysqlContext.get("password");
        String simpleUrl = "jdbc:mysql://" + this.host + ":" + this.port;
        this.jdbcUrl = this.mysqlContext.getString("url", simpleUrl);
    }


    public void initJDBC(boolean checkBinlog) {

        try {
            Map<String, String> config = Maps.newHashMapWithExpectedSize(20);
            config.put("url", this.jdbcUrl);
            config.put("username", this.username);
            config.put("password", this.password);
            config.put("driverClassName", "com.mysql.jdbc.Driver");
            config.put("initialSize", "1");
            config.put("minIdle", "1");
            config.put("maxActive", "10");
            config.put("defaultAutoCommit", "true");
            config.put("minEvictableIdleTimeMillis", "300000");
            config.put("validationQuery", "SELECT 'x' FROME DUAL");
            config.put("testWhileIdle", "true");
            config.put("testOnBorrow", "false");
            config.put("testOnReturn", "false");
            config.put("poolPreparedStatements", "false");
            config.put("maxPoolPreparedStatementPerConnectionSize", "-1");
            config.put("removeAbandonedTimeout", "1200");
            config.put("logAbandoned", "true");
            this.dataSource = DruidDataSourceFactory.createDataSource(config);
            this.serverId = _getServerId();
            this.version = _getMysqlVersion();
            if (checkBinlog) {
                _checkVar("log_bin", "ON");
                _checkVar("binlog_format", "ROW");
                _checkVar("binlog_row_image", "FULL");
                //_checkVar("gtid_mode", "ON");
                //_checkVar("log_slave_updates", "ON");
                //_checkVar("enforce_gtid_consistency", "ON");
            }
        } catch (Exception e) {
            log.error("init jdbc error : ", e);
            Validate.isTrue(false);
        }
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
            this.schema.put(item, table);
        }
    }


    public void config(String dbName, String tableName, String sensitiveColumns) {

        Set<String> ignoreColumnNames = Sets.newHashSet();
        if (!StringUtils.isEmpty(sensitiveColumns)) {
            List<String> columnNames = Util.splitStringWithFilter(sensitiveColumns, "\\s|,", null);
            ignoreColumnNames.addAll(columnNames);
        }
        String dbTableName = dbName + "." + tableName;
        Table table = Table.builder().dbTableName(dbTableName).db(dbName).table(tableName).columns(Lists.newArrayList())
                .ignoreColumnName(ignoreColumnNames).build();
        this.schema.put(dbTableName, table);
    }


    public Table getTable(String dbName, String tableName, boolean forceReload) {

        return getTable(dbName + "." + tableName, forceReload);
    }


    public Table getTable(String dbTableName, boolean forceReload) {

        Table table = this.schema.get(dbTableName);
        if (forceReload || table.getColumns().isEmpty()) {
            return reloadSchema(table);
        }
        return table;
    }


    private Table reloadSchema(Table table) {

        table.getColumns().clear();
        _getColumns(table);
        List<Meta.MetaEntity> entityList = Lists.newArrayList();
        table.getColumns().forEach(column -> entityList.add(new Meta.MetaEntity(column.getName(), column.getTypeRouter().getTargetType())));
        table.setMeta(new Meta(entityList));
        return table;
    }


    private void _getColumns(final Table table) {

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
                    TypeRouter typeRouter = MysqlType.convert(r.getString("DATA_TYPE"), charset);
                    table.getColumns().add(Column.builder().name(r.getString("COLUMN_NAME"))
                                                   .seq(r.getInt("ORDINAL_POSITION"))
                                                   .type(r.getString("DATA_TYPE"))
                                                   .primaryKey("PRI".equals(r.getString("COLUMN_KEY")))
                                                   .charset(charset)
                                                   .ignore(ignore)
                                                   .typeRouter(typeRouter)
                                                   .build());
                }
                return null;
            }
        });
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


    private String _getCharset(final Table table) {

        String sql =
                "SELECT CCSA.CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.TABLES JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS "
                + "CCSA ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE TABLES.TABLE_SCHEMA = ? AND TABLES.TABLE_NAME = ? ";

        String result = executeQuery(sql, new ICallable<String>() {


            @Override public void handleParams(PreparedStatement p) throws Exception {

                p.setString(1, table.getDb());
                p.setString(2, table.getTable());
            }


            @Override public String handleResultSet(ResultSet r) throws Exception {

                return r.next() ? r.getString(1) : null;
            }
        });
        return result;
    }


    private long _getServerId() {

        String sql = "show variables like 'server_id';";
        Long result = executeQuery(sql, r -> r.next() ? r.getLong(2) : null);
        return result;
    }


    private String _getMysqlVersion() {

        String sql = "select version();";
        String result = executeQuery(sql, r -> r.next() ? r.getString(1) : null);
        return result;
    }


    public <E> E executeQuery(String sql, ICallable<E> cal) {

        Connection c = null;
        PreparedStatement p = null;
        try {
            c = this.dataSource.getConnection();
            p = c.prepareStatement(sql);
            cal.handleParams(p);
            ResultSet r = p.executeQuery();
            E e = cal.handleResultSet(r);
            r.close();
            return e;
        } catch (Exception e) {
            log.error("MysqlDataSource.executequery", e);
            Validate.isTrue(false);
        } finally {
            try {
                if (p != null) {
                    p.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (SQLException e) {
                log.error("MysqlDataSource.executequery", e);
            }
        }
        return null;
    }


    public void close() {

        this.schema.clear();
        if (this.dataSource != null) {
            ((DruidDataSource) this.dataSource).close();
        }
    }


    public static String matchCharset(String charset) {

        if (charset == null) {
            return null;
        }
        switch (charset.toLowerCase()) {
            case "utf8":
            case "utf8mb4":
                return "UTF-8";
            case "latin1":
            case "ascii":
                return "Windows-1252";
            case "ucs2":
                return "UTF-16";
            case "ujis":
                return "EUC-JP";
            default:
                return charset;
        }
    }


    public interface ICallable<V> {


        default void handleParams(PreparedStatement p) throws Exception {

        }

        V handleResultSet(ResultSet r) throws Exception;

    }
}
