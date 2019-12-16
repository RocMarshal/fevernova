package com.github.fevernova.mysql;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.task.binlog.util.MysqlDataSource;
import com.github.fevernova.task.binlog.util.schema.Table;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.DeserializationHelper;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
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

        Table table = this.mysql.getTable("test.tb1", true);
        table.getColumns().forEach(column -> System.out.println(column));
    }


    @Test
    public void T_binlog() {

        Pair<EventDeserializer, Map<Long, TableMapEventData>> ps = DeserializationHelper.create();

        BinaryLogClient client = new BinaryLogClient(this.mysql.getHost(), this.mysql.getPort(), this.mysql.getUsername(), this.mysql.getPassword());
        client.setEventDeserializer(ps.getKey());
        client.setBinlogFilename("mysql-bin.000018");
        client.setBinlogPosition(985);
        client.registerEventListener((Event event) -> System.out.println(event.toString()));
        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
