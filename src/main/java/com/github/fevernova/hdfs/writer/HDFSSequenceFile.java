package com.github.fevernova.hdfs.writer;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEvent;
import com.github.fevernova.hdfs.serialization.SequenceFileSerializer;
import com.github.fevernova.hdfs.serialization.SequenceFileSerializerFactory;
import com.github.fevernova.hdfs.serialization.SequenceFileSerializerType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;


@Slf4j
public class HDFSSequenceFile extends AbstractHDFSWriter {


    private FSDataOutputStream outputStream;

    private SequenceFile.Writer writer;

    private String writeFormat;

    private TaskContext serializerContext;

    private SequenceFileSerializer serializer;

    private String tmpPathStr;

    private String targetPathStr;

    private CompressionCodec codec;


    @Override
    public void configure(GlobalContext globalContext, TaskContext context) {

        super.configure(globalContext, context);
        this.writeFormat = context.getString("format", SequenceFileSerializerType.NullWritable.name());
        this.serializerContext = new TaskContext(context.getSubProperties(SequenceFileSerializerFactory.CTX_PREFIX));
        this.serializer = SequenceFileSerializerFactory.getSerializer(this.writeFormat, this.serializerContext);
        super.codecName = context.getString("codec", "default");
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
    public int writeData(DataEvent e) throws IOException {

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
