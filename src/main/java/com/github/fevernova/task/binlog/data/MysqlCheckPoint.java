package com.github.fevernova.task.binlog.data;


import com.github.fevernova.framework.service.checkpoint.CheckPoint;
import lombok.*;


@Setter
@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class MysqlCheckPoint implements CheckPoint {


    private String host;

    private int port;

    private long serverId;

    private String binlogFileName;

    private long binlogPosition;

    private long binlogTimestamp;

    private long globalId;

}
