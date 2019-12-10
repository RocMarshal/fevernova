package com.github.fevernova.hdfs.orc;


import com.github.fevernova.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;


public class C_BytesColumnVector implements Convert {


    @Override
    public void eval(ColumnVector vector, int row, UData uData) {

        BytesColumnVector bv = (BytesColumnVector) vector;
        byte[] value = uData.toBytes();
        if (value == null) {
            bv.vector[row] = null;
            bv.isNull[row] = true;
            bv.noNulls = false;
        } else {
            bv.setVal(row, value);
        }
    }

}
