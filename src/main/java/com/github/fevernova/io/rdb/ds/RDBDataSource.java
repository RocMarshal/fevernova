package com.github.fevernova.io.rdb.ds;


import com.alibaba.druid.pool.DruidDataSource;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.TaskContext;
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


    protected final Map<String, Table> schema = Maps.newConcurrentMap();

    protected TaskContext context;

    protected String username;

    protected String password;

    protected String host;

    protected int port;

    protected DataSource dataSource;

    protected String createTableTemplete;

    protected String escapeLetter;


    public static RDBDataSource createOf(TaskContext context) {

        String type = context.getString("dbtype", "mysql");
        switch (type) {
            case "mysql":
                return new MysqlDataSource(context);
            case "postgresql":
                return new PostgreDataSource(context);
            default:
                Validate.isTrue(false, "RDBDataSource type unknown .");
                return null;
        }
    }


    public RDBDataSource(TaskContext context) {

        this.context = context;
        this.username = context.get("username");
        this.password = context.get("password");
        this.host = context.getString("host", "127.0.0.1");
    }


    public static String buildDbTableName(String db, String table) {

        return db + "." + table;
    }


    public abstract void initDataSource();


    public Table config(String dbName, String tableName, String sensitiveColumns) {

        Set<String> ignoreColumnNames = Sets.newHashSet();
        if (!StringUtils.isEmpty(sensitiveColumns)) {
            List<String> columnNames = Util.splitStringWithFilter(sensitiveColumns, "\\s|,", null);
            ignoreColumnNames.addAll(columnNames);
        }
        String dbTableName = buildDbTableName(dbName, tableName);
        Table table = Table.builder().dbTableName(dbTableName).db(dbName).table(tableName)
                .columns(Lists.newArrayList()).ignoreColumnName(ignoreColumnNames).build();
        reloadSchema(table);
        this.schema.put(dbTableName, table);
        return table;
    }


    public String processExtraSql4Upsert(Table table, String customStr) {

        return customStr;
    }


    protected abstract void reloadSchema(final Table table);


    public Table getTable(String dbTableName, boolean forceReload) {

        Table table = this.schema.get(dbTableName);
        Validate.notNull(table);
        if (forceReload) {
            reloadSchema(table);
        }
        return table;
    }


    public void executeQuery(String sql) {

        try {
            Connection con = this.dataSource.getConnection();
            Statement st = con.createStatement();
            st.execute(sql);
            st.close();
            con.close();
        } catch (Throwable e) {
            log.error("DataSource.executequery", e);
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
            log.error("DataSource.executequery", e);
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
                log.error("DataSource.executequery", e);
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


    public interface ICallable<V> {


        default void handleParams(PreparedStatement p) throws Exception {

        }

        V handleResultSet(ResultSet r) throws Exception;

    }
}
