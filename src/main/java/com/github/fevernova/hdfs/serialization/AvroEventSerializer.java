package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.Configurable;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEvent;

import java.io.IOException;
import java.io.OutputStream;


public class AvroEventSerializer implements DataSerializer, Configurable {


    private final OutputStream out;


    private AvroEventSerializer(OutputStream out) {

        this.out = out;
    }


    @Override
    public void afterCreate() throws IOException {

    }


    @Override
    public int write(DataEvent event) throws IOException {

        return 0;
    }


    @Override
    public void flush() throws IOException {

    }


    @Override
    public void beforeClose() throws IOException {

    }


    @Override
    public void configure(TaskContext context) {

    }


    public static class Builder implements DataSerializer.Builder {


        @Override
        public DataSerializer build(TaskContext context, OutputStream out) {

            AvroEventSerializer writer = new AvroEventSerializer(out);
            writer.configure(context);
            return writer;
        }
    }
}
