package com.github.fevernova.hdfs;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.hdfs.writer.AbstractHDFSWriter;
import com.github.fevernova.hdfs.writer.WriterFactory;
import com.github.fevernova.kafka.data.KafkaData;
import com.google.common.io.Resources;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;


public class T_Writer {


    private GlobalContext globalContext;

    private TaskContext writerContext;

    private KafkaData data;


    @Before
    public void init() {

        JobTags jobTags = JobTags.builder().jobType("testtype").jobId("testid").cluster("testclr").unit(1).level("l3")
                .deployment("testdpl").podName("testpod").podTotalNum(5).podIndex(0).build();
        this.globalContext = GlobalContext.builder().jobTags(jobTags).build();

        this.writerContext = new TaskContext("test");
        this.writerContext.put("username", "hadoop");
        this.writerContext.put("hdfsconfigpath", Paths.get(Resources.getResource("core-site.xml").getPath()).getParent().toString());
        this.writerContext.put("basepath", "/tmp/fevernova/logdist/");
        this.writerContext.put("basetmppath", "/tmp/fevernova/logdist/tmp/");

        this.data = new KafkaData();
        this.data.setValue("abcdefghijklmnopqrstuvwxyz".getBytes());
    }


    @Test
    public void T_DataStream() throws IOException {

        AbstractHDFSWriter writer = WriterFactory.getHDFSWriter(WriterFactory.HDFS_DATASTREAMTYPE);
        writer.setIndex(0);
        writer.configure(this.globalContext, this.writerContext);
        writer.open();
        writer.writeData(data);
        writer.sync();
        Pair<String, String> result = writer.close();
        System.out.println(result);
    }


    @Test
    public void T_CompressedStream() throws IOException {

        AbstractHDFSWriter writer = WriterFactory.getHDFSWriter(WriterFactory.HDFS_COMPSTREAMTYPE);
        writer.setIndex(0);
        writer.configure(this.globalContext, this.writerContext);
        writer.open();
        writer.writeData(data);
        writer.sync();
        Pair<String, String> result = writer.close();
        System.out.println(result);
    }


    @Test
    public void T_SequenceStream() throws IOException {

        AbstractHDFSWriter writer = WriterFactory.getHDFSWriter(WriterFactory.HDFS_SEQUENCEFILETYPE);
        writer.setIndex(0);
        writer.configure(this.globalContext, this.writerContext);
        writer.open();
        writer.writeData(data);
        writer.sync();
        Pair<String, String> result = writer.close();
        System.out.println(result);
    }
}
