package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.Configurable;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;


/**
 * This class simply writes the body of the event to the output stream
 * and appends a newline after each event.
 */
@Slf4j
public class BodyTextEventSerializer implements DataSerializer, Configurable {


    private final OutputStream out;


    private BodyTextEventSerializer(OutputStream out) {

        this.out = out;
    }


    @Override
    public int write(DataEvent event) throws IOException {

        out.write(event.getBytes());
        out.write('\n');
        return event.getBytes().length + 1;
    }


    @Override
    public void configure(TaskContext context) {

    }


    @Override
    public void flush() throws IOException {

    }


    @Override
    public void beforeClose() throws IOException {

    }


    @Override
    public void afterCreate() throws IOException {

    }


    public static class Builder implements DataSerializer.Builder {


        @Override
        public DataSerializer build(TaskContext context, OutputStream out) {

            BodyTextEventSerializer btes = new BodyTextEventSerializer(out);
            btes.configure(context);
            return btes;
        }
    }
}
