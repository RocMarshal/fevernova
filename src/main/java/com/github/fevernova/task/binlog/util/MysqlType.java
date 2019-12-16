package com.github.fevernova.task.binlog.util;


import com.github.fevernova.data.TypeRouter;
import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.impl.*;
import com.github.fevernova.data.type.impl.date.UDate;
import com.github.fevernova.data.type.impl.date.UDateTime;
import com.github.fevernova.data.type.impl.date.UTime;
import com.github.fevernova.data.type.impl.date.UYear;

import java.nio.charset.Charset;


public class MysqlType {


    public static boolean lazy = true;


    public static TypeRouter convert(String mysqlType, Charset charset) {

        switch (mysqlType) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
                return TypeRouter.builder().uData(new UInteger(lazy)).from(MethodType.INT).to(MethodType.INT).targetType(DataType.INT).build();
            case "bigint":
                return TypeRouter.builder().uData(new ULong(lazy)).from(MethodType.LONG).to(MethodType.LONG).targetType(DataType.LONG).build();
            case "float":
                return TypeRouter.builder().uData(new UFloat(lazy)).from(MethodType.FLOAT).to(MethodType.FLOAT).targetType(DataType.FLOAT).build();
            case "double":
                return TypeRouter.builder().uData(new UDouble(lazy)).from(MethodType.DOUBLE).to(MethodType.DOUBLE).targetType(DataType.DOUBLE)
                        .build();


            case "decimal":
            case "enum":
            case "set":
            case "bit":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
            case "binary":
            case "varbinary":
                return TypeRouter.builder().uData(new UString(lazy)).from(MethodType.STRING).to(MethodType.STRING).targetType(DataType.STRING)
                        .build();


            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "varchar":
            case "char":
                return TypeRouter.builder().uData(new UString(lazy, charset.name())).from(MethodType.BYTES).to(MethodType.STRING)
                        .targetType(DataType.STRING).build();
            case "year":
                return TypeRouter.builder().uData(new UYear(true)).from(MethodType.INT).to(MethodType.INT).targetType(DataType.LONG).build();
            case "date":
                return TypeRouter.builder().uData(new UDate(lazy)).from(MethodType.STRING).to(MethodType.LONG).targetType(DataType.LONG).build();
            case "datetime":
                return TypeRouter.builder().uData(new UDateTime(lazy)).from(MethodType.STRING).to(MethodType.LONG).targetType(DataType.LONG).build();
            case "time":
                return TypeRouter.builder().uData(new UTime(lazy)).from(MethodType.STRING).to(MethodType.LONG).targetType(DataType.LONG).build();
            case "timestamp":
                return TypeRouter.builder().uData(new UDateTime(lazy)).from(MethodType.DATE).to(MethodType.LONG).targetType(DataType.LONG).build();
            case "json":
                return TypeRouter.builder().uData(new UBytes(true, charset.name())).from(MethodType.BYTES).to(MethodType.BYTES)
                        .targetType(DataType.BYTES).build();
            case "geometry":
            case "geometrycollection":
            case "linestring":
            case "multilinestring":
            case "multipoint":
            case "multipolygon":
            case "polygon":
            case "point":
            default:
                throw new IllegalArgumentException("unsupported column type " + mysqlType);
        }
    }
}
