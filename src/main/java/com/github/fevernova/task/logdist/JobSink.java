package com.github.fevernova.task.logdist;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.hdfs.AbstractHDFSBatchSink;
import com.github.fevernova.hdfs.writer.WriterFactory;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JobSink extends AbstractHDFSBatchSink {


    private TaskContext writerContext;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.writerContext = new TaskContext("writer", super.taskContext.getSubProperties("writer."));
        super.hdfsWriter = WriterFactory.getHDFSWriter(this.writerContext.getString("writertype", WriterFactory.HDFS_SEQUENCEFILETYPE));
        super.hdfsWriter.setIndex(super.index);
        super.hdfsWriter.configure(super.globalContext, this.writerContext);
    }
}
