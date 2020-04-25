package com.github.fevernova.io.rdb;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.rdb.ds.MysqlDataSource;
import com.github.fevernova.io.rdb.ds.RDBDataSource;
import com.github.fevernova.io.rdb.schema.Table;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;


public class T_Mysql {


    private RDBDataSource mysql;


    @Before
    public void init() {

        TaskContext mysqlContext = new TaskContext("datasource");
        mysqlContext.put("host", "127.0.0.1");
        mysqlContext.put("port", "3306");
        mysqlContext.put("username", "root");
        mysqlContext.put("password", "root");

        Set<String> whiteList = Sets.newHashSet();
        whiteList.add("test.persons");

        this.mysql = new MysqlDataSource(mysqlContext);
        this.mysql.initDataSource();
        this.mysql.config("test", "persons", "");
    }


    @Test
    public void T_reloadSchema() {

        Table table = this.mysql.getTable("test.persons", false);
        table.getColumns().forEach(column -> System.out.println(column));
    }


    @Test
    public void T_loadData() throws SQLException {

        for (int i = 0; i < 1000; i++) {
            Connection c = this.mysql.getDataSource().getConnection();
            c.setAutoCommit(true);
            PreparedStatement p = c.prepareStatement("insert into test.persons(name,age,address,city) values(?,?,?,?)");
            p.setString(1, "n_" + i);
            p.setInt(2, i);
            p.setString(3, "addr_" + i);
            p.setString(4, "ct_" + i);
            int r = p.executeUpdate();
            p.close();
            c.close();
        }
    }

}
