package com.github.fevernova.state;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.hdfs.FileInfo;
import com.github.fevernova.hdfs.HDFSCheckPoint;
import com.github.fevernova.task.binlog.data.MysqlCheckPoint;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;


public class T_State {


    @Test
    public void T_Serializer() {

        StateValue stateValue = new StateValue();
        stateValue.setComponentType(ComponentType.SOURCE);
        stateValue.setComponentTotalNum(3);
        stateValue.setCompomentIndex(1);
        MysqlCheckPoint checkPoint = MysqlCheckPoint.builder()
                .binlogFileName("binlog1")
                .binlogPosition(4)
                .binlogTimestamp(123456789)
                .globalId(123)
                .host("127.0.0.1")
                .port(3306)
                .serverId(12)
                .build();
        stateValue.setValue(checkPoint);
        System.out.println(JSON.toJSONString(stateValue));

        StateValue stateValue1 = new StateValue();
        stateValue1.setComponentType(ComponentType.SINK);
        stateValue1.setComponentTotalNum(2);
        stateValue1.setCompomentIndex(1);
        HDFSCheckPoint checkPoint1 = new HDFSCheckPoint();
        checkPoint1.getFiles().add(FileInfo.builder().from("f0").to("t0").build());
        checkPoint1.getFiles().add(FileInfo.builder().from("f1").to("t1").build());
        stateValue1.setValue(checkPoint1);
        System.out.println(JSON.toJSONString(stateValue1));

        List<StateValue> stateValueList = Lists.newArrayList();
        stateValueList.add(stateValue);
        stateValueList.add(stateValue1);

        String json = JSON.toJSONString(stateValueList);
        List<StateValue> stateValueList1 = JSON.parseArray(json, StateValue.class);

        MysqlCheckPoint r1 = new MysqlCheckPoint();
        r1.parseFromJSON((JSONObject) stateValueList1.get(0).getValue());
        System.out.println(r1);
        HDFSCheckPoint r2 = new HDFSCheckPoint();
        r2.parseFromJSON((JSONObject) stateValueList1.get(1).getValue());
        System.out.println(r2);

    }
}
