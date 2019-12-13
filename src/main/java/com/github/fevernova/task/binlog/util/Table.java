package com.github.fevernova.task.binlog.util;


import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Set;


@Getter
@Builder
public class Table {


    private String dbTableName;

    private String db;

    private String table;

    private String topic;

    private List<Column> columns;

    private Set<String> ignoreColumnName;

}
