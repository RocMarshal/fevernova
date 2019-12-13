package com.github.fevernova.task.binlog.util;


import com.github.fevernova.data.Mapping;
import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.type.MethodType;
import org.apache.commons.lang3.tuple.Triple;


public class MysqlType {


    public static Triple<MethodType, DataType, MethodType> convert(String mysqlType) {

        switch (mysqlType) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
                return Triple.of(MethodType.INT, DataType.INT, Mapping.convert(DataType.INT));
            case "bigint":
                return Triple.of(MethodType.LONG, DataType.LONG, Mapping.convert(DataType.LONG));
            case "float":
                return Triple.of(MethodType.FLOAT, DataType.FLOAT, Mapping.convert(DataType.FLOAT));
            case "double":
                return Triple.of(MethodType.DOUBLE, DataType.DOUBLE, Mapping.convert(DataType.DOUBLE));
            case "decimal":
                return Triple.of(MethodType.STRING, DataType.STRING, Mapping.convert(DataType.STRING));
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "varchar":
            case "char":
                return Triple.of(MethodType.STRING, DataType.STRING, Mapping.convert(DataType.STRING));
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
            case "binary":
            case "varbinary":
                return Triple.of(MethodType.STRING, DataType.STRING, Mapping.convert(DataType.STRING));
            case "date":
            case "datetime":
            case "timestamp":
            case "time":
            case "year":
                return Triple.of(MethodType.DATE, DataType.LONG, Mapping.convert(DataType.LONG));
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
