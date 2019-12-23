package com.github.fevernova.task.binlog.data;


import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.service.checkpoint.CheckPoint;
import lombok.*;


@ToString
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MysqlCheckPoint implements CheckPoint {


    private String host;

    private int port;

    private long serverId;

    private String binlogFileName;

    private long binlogPosition;

    private long binlogTimestamp;

    private long globalId;


    @Override public void parseFromJSON(JSONObject jsonObject) {

        this.host = jsonObject.getString("host");
        this.port = jsonObject.getIntValue("port");
        this.serverId = jsonObject.getLongValue("serverId");
        this.binlogFileName = jsonObject.getString("binlogFileName");
        this.binlogPosition = jsonObject.getLongValue("binlogPosition");
        this.binlogTimestamp = jsonObject.getLongValue("binlogTimestamp");
        this.globalId = jsonObject.getLongValue("globalId");
    }
}
