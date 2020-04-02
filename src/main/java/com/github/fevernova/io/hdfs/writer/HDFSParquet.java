package com.github.fevernova.io.hdfs.writer;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;


public class HDFSParquet extends AbstractHDFSWriter {


    private String tableName;

    private ParquetWriter<GenericData.Record> writer;


    @Override public void configure(GlobalContext globalContext, TaskContext writerContext) {

        super.configure(globalContext, writerContext);
        this.tableName = writerContext.getString("tableName");

    }


    @Override public void open() throws IOException {

    }


    @Override public int writeData(Data event) throws IOException {

        return 0;
    }


    @Override public void sync() throws IOException {

    }


    @Override public Pair<String, String> close() throws IOException {

        return null;
    }
}
