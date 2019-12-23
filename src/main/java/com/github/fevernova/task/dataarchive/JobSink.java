package com.github.fevernova.task.dataarchive;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.hdfs.AbstractHDFSBatchSink;
import com.github.fevernova.hdfs.writer.HDFSOrcFile;
import com.github.fevernova.hdfs.writer.WriterFactory;
import com.github.fevernova.task.dataarchive.schema.SchemaData;


public class JobSink extends AbstractHDFSBatchSink {


    private TaskContext writerContext;


    public JobSink(GlobalContext globalContext,
                   TaskContext taskContext,
                   int index,
                   int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.writerContext = new TaskContext("writer", super.taskContext.getSubProperties("writer."));
        super.hdfsWriter = WriterFactory.getHDFSWriter(super.taskContext.getString("writertype", WriterFactory.HDFS_ORCFILETYPE));
        super.hdfsWriter.setIndex(super.index);
        super.hdfsWriter.configure(super.globalContext, this.writerContext);
    }


    @Override public BroadcastData onBroadcast(BroadcastData broadcastData) {

        ((HDFSOrcFile) super.hdfsWriter).initColumnInfos((SchemaData) broadcastData);
        return null;
    }
}
