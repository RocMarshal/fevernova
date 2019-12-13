package com.github.fevernova.task.binlog.util;


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
}
