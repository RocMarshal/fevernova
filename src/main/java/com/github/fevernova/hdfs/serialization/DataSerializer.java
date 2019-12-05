package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEvent;

import java.io.IOException;
import java.io.OutputStream;


public interface DataSerializer {


    /**
     * {@link TaskContext} prefix
     */
    public static String CTX_PREFIX = "serializer.";

    /**
     * Hook to write a header after file is opened for the first time.
     */
    public void afterCreate() throws IOException;


    /**
     * Serialize and write the given event.
     *
     * @param event Event to write to the underlying stream.
     * @return size of data
     * @throws IOException
     */
    public int write(DataEvent event) throws IOException;

    /**
     * Hook to flush any internal write buffers to the underlying stream.
     * It is NOT necessary for an implementation to then call flush() / sync()
     * on the underlying stream itself, since those semantics would be provided
     * by the driver that calls this API.
     */
    public void flush() throws IOException;

    /**
     * Hook to write a trailer before the stream is closed.
     * Implementations must not buffer data in this call since
     * EventSerializer.flush() is not guaranteed to be called after beforeClose().
     */
    public void beforeClose() throws IOException;

    /**
     * Knows how to construct this event serializer.<br/>
     * <b>Note: Implementations MUST provide a public a no-arg constructor.</b>
     */
    public interface Builder {


        public DataSerializer build(TaskContext context, OutputStream out);
    }
}
