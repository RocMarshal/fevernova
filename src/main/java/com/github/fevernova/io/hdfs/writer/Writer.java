package com.github.fevernova.io.hdfs.writer;


import com.github.fevernova.framework.common.Configurable;
import com.github.fevernova.framework.common.data.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;


public interface Writer extends Configurable {


    void open() throws IOException;

    int writeData(Data event) throws IOException;

    void sync() throws IOException;

    Pair<String, String> close() throws IOException;
}
