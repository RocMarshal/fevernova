package com.github.fevernova.task.binlog.util.schema;


import com.github.fevernova.data.TypeRouter;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;


@Getter
@Builder
@ToString
public class Column {


    private String name;

    private int seq;

    private String type;

    private boolean primaryKey;

    private String charset;

    private boolean ignore;

    private TypeRouter typeRouter;

}
