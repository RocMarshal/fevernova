package com.github.fevernova.task.binlog.util;


import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.data.type.impl.*;
import com.github.fevernova.data.type.impl.date.UDate;
import org.apache.commons.lang3.tuple.Triple;


public class MysqlType {

    //TODO 需要调整

    public static boolean lazy = false;


    public static Triple<MethodType, UData, DataType> convert(String mysqlType) {

        switch (mysqlType) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
                return Triple.of(MethodType.INT, new UInteger(lazy), DataType.INT);
            case "bigint":
                return Triple.of(MethodType.LONG, new ULong(lazy), DataType.LONG);
            case "float":
                return Triple.of(MethodType.FLOAT, new UFloat(lazy), DataType.FLOAT);
            case "double":
                return Triple.of(MethodType.DOUBLE, new UDouble(lazy), DataType.DOUBLE);
            case "decimal":
                return Triple.of(MethodType.STRING, new UString(lazy), DataType.STRING);
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "varchar":
            case "char":
                return Triple.of(MethodType.STRING, new UString(lazy), DataType.STRING);
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
            case "binary":
            case "varbinary":
                return Triple.of(MethodType.STRING, new UString(lazy), DataType.STRING);
            case "date":
            case "datetime":
            case "time":
            case "year":
                return Triple.of(MethodType.STRING, new UDate(lazy), DataType.LONG);
            case "timestamp":
                return Triple.of(MethodType.DATE, new UDate(lazy), DataType.LONG);
            case "enum":
            case "set":
            case "bit":
            case "json":
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
