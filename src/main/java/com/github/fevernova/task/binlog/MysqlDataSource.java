package com.github.fevernova.task.binlog;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.github.fevernova.framework.common.context.TaskContext;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;


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


    public MysqlDataSource(TaskContext mysqlContext) {

        this.host = mysqlContext.get("host");
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
        this.serverId = getServerId();
        this.mysqlVersion = getMysqlVersion();
    }


    public long getServerId() {

        String sql = "show variables like 'server_id';";
        Long result = executeQuery(sql, new ResultSetICallable<Long>() {


            @Override
            public Long handleResultSet(ResultSet r) throws Exception {

                r.next();
                return r.getLong(2);
            }
        });
        return result;
    }


    public String getMysqlVersion() {

        String sql = "select version();";
        String result = executeQuery(sql, new ResultSetICallable<String>() {


            @Override
            public String handleResultSet(ResultSet r) throws Exception {

                r.next();
                return r.getString(1);
            }
        });
        return result;
    }


    private String wrap(String str) {

        return "`" + str + "`";
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
