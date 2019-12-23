package com.github.fevernova.hdfs.orc;


import com.github.fevernova.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;


public class C_DoubleColumnVector implements Convert {


    @Override
    public void eval(ColumnVector vector, int row, UData uData) {

        DoubleColumnVector dv = (DoubleColumnVector) vector;
        Double value = uData.toDouble();
        if (value == null) {
            dv.vector[row] = DoubleColumnVector.NULL_VALUE;
            dv.isNull[row] = true;
            dv.noNulls = false;
        } else {
            dv.vector[row] = value;
        }
    }

}
