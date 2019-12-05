package com.github.fevernova.hdfs.writer;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEvent;
import com.github.fevernova.hdfs.serialization.DataSerializer;
import com.github.fevernova.hdfs.serialization.DataSerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


@Slf4j
public class HDFSDataStream extends AbstractHDFSWriter {


    private FSDataOutputStream outputStream;

    private String serializerType;

    private TaskContext serializerContext;

    private DataSerializer serializer;

    private String tmpPathStr;

    private String targetPathStr;


    @Override
    public void configure(GlobalContext globalContext, TaskContext context) {

        super.configure(globalContext, context);
        this.serializerType = context.getString("serializer", "TEXT");
        this.serializerContext = new TaskContext(context.getSubProperties(DataSerializer.CTX_PREFIX));
    }


    @Override
    public void open() throws IOException {

        this.tmpPathStr = assemblePath(super.baseTmpPath, ".log");
        log.info("LOG_DIST : create or open tmp file path :" + tmpPathStr);
        this.targetPathStr = assemblePath(super.basePath, ".log");

        Path destinationPath = new Path(this.tmpPathStr);
        if (!super.fileSystem.isFile(destinationPath)) {
            super.fileSystem.createNewFile(destinationPath);
        }
        this.outputStream = super.fileSystem.create(destinationPath);

        this.serializer = DataSerializerFactory.getInstance(this.serializerType, this.serializerContext, this.outputStream);
        this.serializer.afterCreate();
    }


    @Override
    public int writeData(DataEvent event) throws IOException {

        return this.serializer.write(event);
    }


    @Override
    public void sync() throws IOException {

        this.serializer.flush();
        this.outputStream.hflush();
    }


    @Override
    public Pair<String, String> close() throws IOException {

        this.serializer.flush();
        this.serializer.beforeClose();
        this.outputStream.hflush();
        this.outputStream.close();
        return Pair.of(this.tmpPathStr, this.targetPathStr);
    }
}
