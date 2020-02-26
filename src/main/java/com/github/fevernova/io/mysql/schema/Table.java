package com.github.fevernova.io.mysql.schema;


import com.github.fevernova.io.data.message.Meta;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

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

    @Setter
    private Meta meta;

}
