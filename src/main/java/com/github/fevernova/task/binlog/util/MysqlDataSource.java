package com.github.fevernova.task.binlog.util;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.github.fevernova.data.Mapping;
import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.message.Meta;
import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.task.binlog.util.schema.Column;
import com.github.fevernova.task.binlog.util.schema.Table;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Triple;

import javax.sql.DataSource;
import java.nio.charset.Charset;
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


    private String host;

    private int port;

    private long serverId;

    private long slaveId;

    private String username;

    private String password;

    private String jdbcUrl;

    private DataSource dataSource;

    private String mysqlVersion;

    private Map<String, Table> schema = Maps.newConcurrentMap();


    public MysqlDataSource(TaskContext mysqlContext) {

        this.host = mysqlContext.getString("host","127.0.0.1");
        this.port = mysqlContext.getInteger("port", 3306);
        this.slaveId = mysqlContext.getLong("slaveid");
        this.username = mysqlContext.get("username");
        this.password = mysqlContext.get("password");
    }


    public void initJDBC() throws Exception {

        this.jdbcUrl = "jdbc:mysql://" + this.host + ":" + this.port;
        Map<String, String> config = Maps.newHashMapWithExpectedSize(20);
        config.put("url", this.jdbcUrl);
        config.put("username", this.username);
        config.put("password", this.password);
        config.put("driverClassName", "com.mysql.jdbc.Driver");
        config.put("initialSize", "1");
        config.put("minIdle", "1");
        config.put("maxActive", "3");
        config.put("defaultAutoCommit", "true");
        config.put("minEvictableIdleTimeMillis", "300000");
        config.put("validationQuery", "SELECT 'x' FROME DUAL");
        config.put("testWhileIdle", "true");
        config.put("testOnBorrow", "false");
        config.put("testOnReturn", "false");
        config.put("poolPreparedStatements", "false");
        config.put("maxPoolPreparedStatementPerConnectionSize", "-1");
        config.put("removeAbandoned", "true");
        config.put("removeAbandonedTimeout", "1200");
        config.put("logAbandoned", "true");
        this.dataSource = DruidDataSourceFactory.createDataSource(config);
        this.serverId = _getServerId();
        this.mysqlVersion = _getMysqlVersion();
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


    public Table getTable(String dbTableName) {

        return this.schema.get(dbTableName);
    }


    public Table reloadSchema(String dbTableName) {

        Table table = this.schema.get(dbTableName);
        table.getColumns().clear();
        _getColumns(table);
        List<Meta.MetaEntity> entityList = Lists.newArrayList();
        table.getColumns().forEach(column -> entityList.add(new Meta.MetaEntity(column.getName(), column.getTargetType())));
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
                    Triple<MethodType, UData, DataType> typeTriple = MysqlType.convert(r.getString("DATA_TYPE"));

                    table.getColumns().add(Column.builder().name(r.getString("COLUMN_NAME"))
                                                   .seq(r.getInt("ORDINAL_POSITION"))
                                                   .type(r.getString("DATA_TYPE"))
                                                   .primaryKey("PRI".equals(r.getString("COLUMN_KEY")))
                                                   .charset(_matchCharset(r.getString("CHARACTER_SET_NAME")))
                                                   .ignore(ignore)
                                                   .uData(typeTriple.getMiddle())
                                                   .from(typeTriple.getLeft())
                                                   .to(Mapping.convert(typeTriple.getRight()))
                                                   .targetType(typeTriple.getRight())
                                                   .build());
                }
                return null;
            }
        });
    }


    private Charset _matchCharset(String charset) {

        if (charset == null) {
            return null;
        }
        switch (charset.toLowerCase()) {
            case "utf8":
            case "utf8mb4":
                return Charset.forName("UTF-8");
            case "latin1":
            case "ascii":
                return Charset.forName("Windows-1252");
            case "ucs2":
                return Charset.forName("UTF-16");
            case "ujis":
                return Charset.forName("EUC-JP");
            default:
                try {
                    return Charset.forName(charset.toLowerCase());
                } catch (java.nio.charset.UnsupportedCharsetException e) {
                    throw new RuntimeException("error: unhandled character set '" + charset + "'");
                }
        }
    }


    private Charset _getCharset(final Table table) {

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
        return _matchCharset(result);
    }


    private long _getServerId() {

        String sql = "show variables like 'server_id';";
        Long result = executeQuery(sql, new ResultSetICallable<Long>() {


            @Override
            public Long handleResultSet(ResultSet r) throws Exception {

                return r.next() ? r.getLong(2) : null;
            }
        });
        return result;
    }


    private String _getMysqlVersion() {

        String sql = "select version();";
        String result = executeQuery(sql, new ResultSetICallable<String>() {


            @Override
            public String handleResultSet(ResultSet r) throws Exception {

                return r.next() ? r.getString(1) : null;
            }
        });
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


    abstract class ICallable<V> {


        public abstract void handleParams(PreparedStatement p) throws Exception;

        public abstract V handleResultSet(ResultSet r) throws Exception;

    }


    abstract class ResultSetICallable<V> extends ICallable<V> {


        @Override
        public void handleParams(PreparedStatement p) throws Exception {

        }


        @Override public abstract V handleResultSet(ResultSet r) throws Exception;
    }
}
