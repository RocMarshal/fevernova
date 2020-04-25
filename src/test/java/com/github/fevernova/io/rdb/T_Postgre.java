package com.github.fevernova.io.rdb;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.rdb.ds.PostgreDataSource;
import com.github.fevernova.io.rdb.ds.RDBDataSource;
import com.github.fevernova.io.rdb.schema.Table;
import org.junit.Before;
import org.junit.Test;


public class T_Postgre {


    private RDBDataSource postgre;


    @Before
    public void init() {

        TaskContext context = new TaskContext("datasource");
        context.put("host", "127.0.0.1");
        context.put("port", "5431");
        context.put("username", "postgres");
        context.put("password", "postgres");
        context.put("dbtype", "postgresql");

        this.postgre = new PostgreDataSource(context);
        this.postgre.initDataSource();
        this.postgre.config("public", "persons", "");
    }


    @Test
    public void T_reloadSchema() {

        Table table = this.postgre.getTable("public.persons", false);
        table.getColumns().forEach(column -> System.out.println(column));
    }

}
