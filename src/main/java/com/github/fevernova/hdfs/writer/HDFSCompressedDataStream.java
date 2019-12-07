package com.github.fevernova.hdfs.writer;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.hdfs.serialization.DataSerializer;
import com.github.fevernova.hdfs.serialization.DataSerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;


@Slf4j
public class HDFSCompressedDataStream extends AbstractHDFSWriter {


    private String serializerType;

    private TaskContext serializerContext;

    private DataSerializer serializer;

    private FSDataOutputStream outputStream;

    private CompressionCodec codec;

    private Compressor compressor;

    private CompressionOutputStream compressionOutputStream;

    private boolean isFinished = false;

    private String tmpPathStr;

    private String targetPathStr;


    @Override
    public void configure(GlobalContext globalContext, TaskContext writerContext) {

        super.configure(globalContext, writerContext);
        this.serializerContext = new TaskContext(writerContext.getSubProperties(DataSerializer.CTX_PREFIX));
        this.serializerType = writerContext.getString("type", "TEXT");
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

        if (this.compressor == null && needCompressor()) {
            this.compressor = CodecPool.getCompressor(this.codec, super.configuration);
        }
        this.compressionOutputStream = this.codec.createOutputStream(this.outputStream, this.compressor);

        this.serializer = DataSerializerFactory.getInstance(this.serializerType, this.serializerContext, this.compressionOutputStream);
        this.serializer.afterCreate();
        this.isFinished = false;
    }


    @Override
    public int writeData(Data event) throws IOException {

        if (this.isFinished) {
            this.compressionOutputStream.resetState();
            this.isFinished = false;
        }
        return this.serializer.write(event);
    }


    @Override
    public void sync() throws IOException {

        this.serializer.flush();
        if (!this.isFinished) {
            this.compressionOutputStream.finish();
            this.isFinished = true;
        }
        this.outputStream.hflush();
    }


    @Override
    public Pair<String, String> close() throws IOException {

        this.serializer.flush();
        this.serializer.beforeClose();
        if (!this.isFinished) {
            this.compressionOutputStream.finish();
            this.isFinished = true;
        }
        this.outputStream.hflush();
        this.compressionOutputStream.close();
        CodecPool.returnCompressor(this.compressor);
        this.compressor = null;
        return Pair.of(this.tmpPathStr, this.targetPathStr);
    }
}
