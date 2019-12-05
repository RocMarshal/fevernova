package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEvent;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.Collections;


public class HDFSWritableSerializer implements SequenceFileSerializer {


    private BytesWritable makeByteWritable(DataEvent e) {

        BytesWritable bytesObject = new BytesWritable();
        bytesObject.set(e.getBytes(), 0, e.getBytes().length);
        return bytesObject;
    }


    @Override
    public Class<LongWritable> getKeyClass() {

        return LongWritable.class;
    }


    @Override
    public Class<BytesWritable> getValueClass() {

        return BytesWritable.class;
    }


    @Override
    public Iterable<Record> serialize(DataEvent e) {

        Object key = getKey(e);
        Object value = getValue(e);
        return Collections.singletonList(new Record(key, value));
    }


    private Object getKey(DataEvent e) {

        long eventStamp = e.getTimestamp();
        return new LongWritable(eventStamp);
    }


    private Object getValue(DataEvent e) {

        return makeByteWritable(e);
    }


    public static class Builder implements SequenceFileSerializer.Builder {


        @Override
        public SequenceFileSerializer build(TaskContext context) {

            return new HDFSWritableSerializer();
        }

    }

}
