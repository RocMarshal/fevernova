package com.github.fevernova.mysql;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.task.binlog.util.MysqlDataSource;
import com.github.fevernova.task.binlog.util.schema.Table;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;


public class T_DataSource {


    private MysqlDataSource mysql;


    @Before
    public void init() {

        TaskContext mysqlContext = new TaskContext("mysql");
        mysqlContext.put("host", "127.0.0.1");
        mysqlContext.put("port", "3306");
        mysqlContext.put("slaveid", "16384");
        mysqlContext.put("username", "root");
        mysqlContext.put("password", "root");

        Set<String> whiteList = Sets.newHashSet();
        whiteList.add("test.tb1");

        this.mysql = new MysqlDataSource(mysqlContext);
        this.mysql.config(whiteList, Maps.newHashMap());

        try {
            this.mysql.initJDBC();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void T_reloadSchema() {

        Table table = this.mysql.reloadSchema("test.tb1");
        table.getColumns().forEach(column -> System.out.println(column));
    }


    @Test
    public void T_binlog() {

        BinaryLogClient client = new BinaryLogClient(this.mysql.getHost(), this.mysql.getPort(), this.mysql.getUsername(), this.mysql.getPassword());
        client.setBinlogFilename("mysql-bin.000017");
        client.setBinlogPosition(4);
        client.registerEventListener((Event event) -> System.out.println(event.toString()));
        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
