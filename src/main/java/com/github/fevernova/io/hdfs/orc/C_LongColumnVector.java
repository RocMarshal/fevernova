package com.github.fevernova.io.hdfs.orc;


import com.github.fevernova.io.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;


public class C_LongColumnVector implements Convert {


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
