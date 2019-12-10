package com.github.fevernova.hdfs.orc;


import com.github.fevernova.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;


public interface Convert {


    void eval(ColumnVector vector, int row, UData uData);

}
