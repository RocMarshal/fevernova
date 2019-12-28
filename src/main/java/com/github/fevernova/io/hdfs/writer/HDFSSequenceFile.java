package com.github.fevernova.io.hdfs.writer;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.io.hdfs.serialization.SequenceFileSerializer;
import com.github.fevernova.io.hdfs.serialization.SequenceFileSerializerFactory;
import com.github.fevernova.io.hdfs.serialization.SequenceFileSerializerType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;


@Slf4j
public class HDFSSequenceFile extends AbstractHDFSWriter {


    private String serializerType;

    private TaskContext serializerContext;

    private SequenceFileSerializer serializer;

    private FSDataOutputStream outputStream;

    private CompressionCodec codec;

    private SequenceFile.Writer writer;

    private String tmpPathStr;

    private String targetPathStr;


    @Override
    public void configure(GlobalContext globalContext, TaskContext writerContext) {

        super.configure(globalContext, writerContext);
        this.serializerContext = new TaskContext(writerContext.getSubProperties(SequenceFileSerializerFactory.CTX_PREFIX));
        this.serializerType = writerContext.getString("type", SequenceFileSerializerType.NullWritable.name());
        this.serializer = SequenceFileSerializerFactory.getSerializer(this.serializerType, this.serializerContext);
        this.codec = getCodec();
    }


    @Override
    public void open() throws IOException {

        this.tmpPathStr = assemblePath(super.baseTmpPath, this.codec.getDefaultExtension());
        log.info("LOG_DIST : create or open tmp file path :" + tmpPathStr);
        this.targetPathStr = assemblePath(super.basePath, this.codec.getDefaultExtension());

        Path destinationPath = new Path(this.tmpPathStr);
        if (!super.fileSystem.isFile(destinationPath)) {
            super.fileSystem.createNewFile(destinationPath);
        }
        this.outputStream = super.fileSystem.create(destinationPath);
        this.writer = SequenceFile.createWriter(super.configuration, this.outputStream, this.serializer.getKeyClass(), this
                .serializer.getValueClass(), SequenceFile.CompressionType.RECORD, this.codec);
    }


    @Override
    public int writeData(Data e) throws IOException {

        for (SequenceFileSerializer.Record record : this.serializer.serialize(e)) {
            this.writer.append(record.getKey(), record.getValue());
        }
        return e.getBytes().length;
    }


    @Override
    public void sync() throws IOException {

        this.writer.sync();
        this.outputStream.hflush();
    }


    @Override
    public Pair<String, String> close() throws IOException {

        this.writer.close();
        this.outputStream.hflush();
        this.outputStream.close();
        return Pair.of(this.tmpPathStr, this.targetPathStr);
    }
}
