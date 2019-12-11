package com.github.fevernova.framework.common.data.list;


import com.github.fevernova.framework.common.data.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;


@Getter
@Setter
public class ListData implements Data {


    private List<Object> values;


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
