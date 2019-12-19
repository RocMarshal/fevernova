package com.github.fevernova.task.binlog.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.shyiko.mysql.binlog.event.Event;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Setter
@Getter
@ToString
public class BinlogData implements Data {


    private String dbTableName;

    private Event tablemap;

    private Event event;

    private long timestamp;

    private boolean reloadSchemaCache;

    private long globalId;

    private int rowsNum;


    @Override public void clearData() {

        this.dbTableName = null;
        this.tablemap = null;
        this.event = null;
        this.timestamp = 0;
        this.reloadSchemaCache = false;
        this.globalId = 0;
        this.rowsNum = 0;
    }


    @Override public byte[] getBytes() {

        return new byte[0];
    }
}
