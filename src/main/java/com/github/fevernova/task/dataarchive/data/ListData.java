package com.github.fevernova.task.dataarchive.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.io.data.type.MethodType;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;


@Getter
@Setter
public class ListData implements Data {


    private List<Pair<MethodType, Object>> values;


    @Override public void clearData() {

        this.values.clear();
    }


    @Override public byte[] getBytes() {

        return new byte[0];
    }


    @Override public long getTimestamp() {

        return 0;
    }
}
