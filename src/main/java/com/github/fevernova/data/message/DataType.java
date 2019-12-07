package com.github.fevernova.data.message;


import com.github.fevernova.framework.common.FNException;
import org.apache.avro.Schema;


public enum DataType {

    LONG(Schema.Type.LONG, 0),
    STRING(Schema.Type.STRING, 1),
    INT(Schema.Type.INT, 2),
    DOUBLE(Schema.Type.DOUBLE, 3),
    FLOAT(Schema.Type.FLOAT, 4),
    BYTES(Schema.Type.BYTES, 5),
    BOOLEAN(Schema.Type.BOOLEAN, 6);

    public final Schema.Type avroType;

    public final int index;


    private DataType(Schema.Type avroType, int index) {

        this.avroType = avroType;
        this.index = index;
    }


    public static DataType location(int i) {

        switch (i) {
            case 0:
                return LONG;
            case 1:
                return STRING;
            case 2:
                return INT;
            case 3:
                return DOUBLE;
            case 4:
                return FLOAT;
            case 5:
                return BYTES;
            case 6:
                return BOOLEAN;
            default:
                throw new FNException("DataType Error : " + i);
        }
    }

}
