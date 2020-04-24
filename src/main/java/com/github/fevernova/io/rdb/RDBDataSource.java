package com.github.fevernova.io.rdb;


import com.alibaba.druid.pool.DruidDataSource;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.mysql.MysqlDataSource;
import com.github.fevernova.io.rdb.schema.Table;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Getter
@Slf4j
public abstract class RDBDataSource {


    protected TaskContext context;

    protected String host;

    protected int port;

    protected String username;

    protected String password;

    protected DataSource dataSource;

    protected Map<String, Table> schema = Maps.newConcurrentMap();


    public RDBDataSource(TaskContext context) {

        this.context = context;
        this.host = context.getString("host", "127.0.0.1");
        this.port = context.getInteger("port", 3306);
        this.username = context.get("username");
        this.password = context.get("password");
    }


    public abstract void initDataSource();


    public void config(String dbName, String tableName, String sensitiveColumns) {

        Set<String> ignoreColumnNames = Sets.newHashSet();
        if (!StringUtils.isEmpty(sensitiveColumns)) {
            List<String> columnNames = Util.splitStringWithFilter(sensitiveColumns, "\\s|,", null);
            ignoreColumnNames.addAll(columnNames);
        }
        String dbTableName = buildDbTableName(dbName, tableName);
        Table table = Table.builder().dbTableName(dbTableName).db(dbName).table(tableName).columns(Lists.newArrayList())
                .ignoreColumnName(ignoreColumnNames).build();
        this.schema.put(dbTableName, table);
    }


    public Table getTable(String dbName, String tableName, boolean forceReload) {

        return getTable(buildDbTableName(dbName, tableName), forceReload);
    }


    public Table getTable(String dbTableName, boolean forceReload) {

        Table table = this.schema.get(dbTableName);
        Validate.notNull(table);
        if (forceReload || table.getColumns().isEmpty()) {
            reloadSchema(table);
        }
        return table;
    }


    protected abstract void reloadSchema(final Table table);


    public void executeQuery(String sql) {

        try {
            Connection con = this.dataSource.getConnection();
            Statement st = con.createStatement();
            st.execute(sql);
            st.close();
            con.close();
        } catch (Throwable e) {
            log.error("MysqlDataSource.executequery", e);
            e.printStackTrace();
            Validate.isTrue(false);
        }
    }


    public <E> E executeQuery(String sql, MysqlDataSource.ICallable<E> cal) {

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


    public static String buildDbTableName(String db, String table) {

        return db + "." + table;
    }


    public interface ICallable<V> {


        default void handleParams(PreparedStatement p) throws Exception {

        }

        V handleResultSet(ResultSet r) throws Exception;

    }
}
