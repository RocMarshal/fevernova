package com.github.fevernova.hdfs.writer;


import com.github.fevernova.framework.common.Configurable;
import com.github.fevernova.framework.common.data.DataEvent;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;


public interface Writer extends Configurable {


    void open() throws IOException;

    int writeData(DataEvent event) throws IOException;

    void sync() throws IOException;

    Pair<String, String> close() throws IOException;
}
