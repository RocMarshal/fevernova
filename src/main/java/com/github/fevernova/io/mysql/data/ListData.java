package com.github.fevernova.io.mysql.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.io.mysql.schema.Column;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;


@Getter
@Setter
public class ListData implements Data {


    private List<Pair<Column, Object>> values = Lists.newArrayList();


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
