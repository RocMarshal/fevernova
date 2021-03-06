package com.github.fevernova.io.rdb.ds;


import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.rdb.schema.Column;
import com.github.fevernova.io.rdb.schema.Table;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;


@Slf4j
public class MysqlDataSource extends RDBDataSource {


    public MysqlDataSource(TaskContext context) {

        super(context);
        super.port = context.getInteger("port", 3306);
        super.createTableTemplete = "CREATE TABLE IF NOT EXISTS %s.%s like %s.%s ";
        super.escapeLetter = "`";
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


    @Override
    public void initDataSource() {

        String jdbcUrl = super.context.getString("url", "jdbc:mysql://" + super.host + ":" + super.port);
        try {
            Map<String, String> config = Maps.newHashMapWithExpectedSize(20);
            config.put("url", jdbcUrl);
            config.put("username", super.username);
            config.put("password", super.password);
            config.put("driverClassName", "com.mysql.jdbc.Driver");
            config.put("initialSize", "1");
            config.put("minIdle", "1");
            config.put("maxActive", "10");
            config.put("defaultAutoCommit", "true");
            config.put("minEvictableIdleTimeMillis", "300000");
            config.put("validationQuery", "SELECT 'x' FROM DUAL");
            config.put("testWhileIdle", "true");
            config.put("testOnBorrow", "false");
            config.put("testOnReturn", "false");
            config.put("poolPreparedStatements", "false");
            config.put("maxPoolPreparedStatementPerConnectionSize", "-1");
            config.put("removeAbandonedTimeout", "1200");
            config.put("logAbandoned", "true");
            super.dataSource = DruidDataSourceFactory.createDataSource(config);
        } catch (Exception e) {
            log.error("init jdbc error : ", e);
            Validate.isTrue(false);
        }
    }


    protected void reloadSchema(final Table table) {

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
                    table.getColumns().add(Column.builder().name(r.getString("COLUMN_NAME"))
                                                   .seq(r.getInt("ORDINAL_POSITION"))
                                                   .type(r.getString("DATA_TYPE"))
                                                   .primaryKey("PRI".equals(r.getString("COLUMN_KEY")))
                                                   .charset(r.getString("CHARACTER_SET_NAME"))
                                                   .ignore(ignore)
                                                   .escapeLetter(getEscapeLetter())
                                                   .build());
                }
                return null;
            }
        });
    }
}
