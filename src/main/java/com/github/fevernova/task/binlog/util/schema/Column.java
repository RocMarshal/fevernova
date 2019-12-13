package com.github.fevernova.task.binlog.util.schema;


import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.nio.charset.Charset;


@Getter
@Builder
@ToString
public class Column {


    private String name;

    private int seq;

    private String type;

    private boolean primaryKey;

    private Charset charset;

    private boolean ignore;

    // 类型处理
    private UData uData;

    private MethodType from;

    private MethodType to;

    private DataType targetType;
}
