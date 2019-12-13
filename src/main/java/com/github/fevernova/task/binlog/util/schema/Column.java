package com.github.fevernova.task.binlog.util.schema;


import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.type.MethodType;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Triple;

import java.nio.charset.Charset;


@Getter
@Builder
@ToString
public class Column {


    private String name;

    private int seq;

    private String type;

    private Triple<MethodType, DataType, MethodType> typeEnum;

    private boolean primaryKey;

    private Charset charset;

    private boolean ignore;
}
