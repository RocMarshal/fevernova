package com.github.fevernova.io.mysql;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.rdb.ds.BinlogDataSource;
import com.github.fevernova.io.rdb.schema.Table;
import com.github.fevernova.task.binlog.util.MysqlBinlogType;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;


public class T_DataSource {


    private BinlogDataSource mysql;


    @Before
    public void init() {

        TaskContext mysqlContext = new TaskContext("mysql");
        mysqlContext.put("host", "127.0.0.1");
        mysqlContext.put("port", "3306");
        mysqlContext.put("slaveid", "16384");
        mysqlContext.put("username", "root");
        mysqlContext.put("password", "root");

        Set<String> whiteList = Sets.newHashSet();
        whiteList.add("test.persons");

        this.mysql = new BinlogDataSource(mysqlContext, new MysqlBinlogType());
        this.mysql.config(whiteList, Maps.newHashMap());
        this.mysql.initDataSource();
    }


    @Test
    public void T_reloadSchema() {

        Table table = this.mysql.getTable("test.persons", true);
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
