package com.github.fevernova.hdfs.orc;


import com.github.fevernova.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;


public class C_BooleanColumnVector implements Convert {


    @Override
    public void eval(ColumnVector vector, int row, UData uData) {

        LongColumnVector lv = (LongColumnVector) vector;
        Long value = uData.toLong();
        if (value == null) {
            lv.vector[row] = LongColumnVector.NULL_VALUE;
            lv.isNull[row] = true;
            lv.noNulls = false;
        } else {
            lv.vector[row] = value;
        }
    }

}
