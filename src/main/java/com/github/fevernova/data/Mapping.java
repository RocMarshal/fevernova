package com.github.fevernova.data;


import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.type.MethodType;
import com.google.common.collect.Maps;

import java.util.Map;


public class Mapping {


    private static final Map<DataType, MethodType> mapping = Maps.newHashMap();

    static {
        mapping.put(DataType.LONG, MethodType.LONG);
        mapping.put(DataType.STRING, MethodType.STRING);
        mapping.put(DataType.INT, MethodType.INT);
        mapping.put(DataType.DOUBLE, MethodType.DOUBLE);
        mapping.put(DataType.FLOAT, MethodType.FLOAT);
        mapping.put(DataType.BYTES, MethodType.BYTES);
        mapping.put(DataType.BOOLEAN, MethodType.BOOLEAN);
    }

    public static MethodType convert(DataType dataType) {

        return mapping.get(dataType);
    }
}
