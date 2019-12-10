package com.github.fevernova.hdfs.orc;


import com.github.fevernova.data.type.UData;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;


public class C_DecimalColumnVector implements Convert {


    @Override
    public void eval(ColumnVector vector, int row, UData uData) {

        DecimalColumnVector dv = (DecimalColumnVector) vector;
        String value = uData.toStr();
        if (value == null) {
            dv.setNullDataValue(row);
        } else {
            dv.set(row, HiveDecimal.create(value));
        }
    }

}
