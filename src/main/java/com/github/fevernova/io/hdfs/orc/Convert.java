package com.github.fevernova.io.hdfs.orc;


import com.github.fevernova.io.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;


public interface Convert {


    void eval(ColumnVector vector, int row, UData uData);

}
