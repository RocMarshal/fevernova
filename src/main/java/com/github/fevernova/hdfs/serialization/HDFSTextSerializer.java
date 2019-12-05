package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Collections;


public class HDFSTextSerializer implements SequenceFileSerializer {


    private Text makeText(Data e) {

        Text textObject = new Text();
        textObject.set(e.getBytes(), 0, e.getBytes().length);
        return textObject;
    }


    @Override
    public Class<LongWritable> getKeyClass() {

        return LongWritable.class;
    }


    @Override
    public Class<Text> getValueClass() {

        return Text.class;
    }


    @Override
    public Iterable<Record> serialize(Data e) {

        Object key = getKey(e);
        Object value = getValue(e);
        return Collections.singletonList(new Record(key, value));
    }


    private Object getKey(Data e) {

        long eventStamp = e.getTimestamp();
        return new LongWritable(eventStamp);
    }


    private Object getValue(Data e) {

        return makeText(e);
    }


    public static class Builder implements SequenceFileSerializer.Builder {


        @Override
        public SequenceFileSerializer build(TaskContext context) {

            return new HDFSTextSerializer();
        }

    }

}
