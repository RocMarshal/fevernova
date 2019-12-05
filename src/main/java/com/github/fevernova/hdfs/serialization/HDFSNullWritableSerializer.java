package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Collections;


public class HDFSNullWritableSerializer implements SequenceFileSerializer {


    @Override
    public Class<NullWritable> getKeyClass() {

        return NullWritable.class;
    }


    @Override
    public Class<?> getValueClass() {

        return BytesWritable.class;
    }


    @Override
    public Iterable<Record> serialize(Data e) {

        return Collections.singletonList(new Record(getKey(e), getValue(e)));
    }


    private Object getValue(Data e) {

        return makeByteWritable(e);
    }


    private Object getKey(Data e) {

        return NullWritable.get();
    }


    private BytesWritable makeByteWritable(Data e) {

        BytesWritable bytesObject = new BytesWritable();
        bytesObject.set(e.getBytes(), 0, e.getBytes().length);
        return bytesObject;
    }


    public static class Builder implements SequenceFileSerializer.Builder {


        @Override
        public SequenceFileSerializer build(TaskContext context) {

            return new HDFSNullWritableSerializer();
        }

    }
}
