package com.github.fevernova.io.hdfs.orc;


import com.github.fevernova.io.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

import java.sql.Timestamp;


public class C_TimeStampColumnVector implements Convert {


    @Override
    public void eval(ColumnVector vector, int row, UData uData) {

        TimestampColumnVector tv = (TimestampColumnVector) vector;
        Long value = uData.toLong();
        if (value == null) {
            tv.setNullValue(row);
        } else {
            tv.set(row, new Timestamp(value));
        }
    }

}
