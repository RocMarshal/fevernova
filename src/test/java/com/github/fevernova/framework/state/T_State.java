package com.github.fevernova.framework.state;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.service.checkpoint.MapCheckPoint;
import com.github.fevernova.framework.service.state.StateValue;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.Validate;
import org.junit.Test;

import java.util.List;


public class T_State {


    private static final String KEY = "name";

    private static final String VALUE = "fevernova";


    @Test
    public void T_serializer() {

        StateValue stateValue = new StateValue();
        stateValue.setComponentType(ComponentType.SOURCE);
        stateValue.setComponentTotalNum(2);
        stateValue.setCompomentIndex(0);
        MapCheckPoint mapCheckPoint = new MapCheckPoint();
        mapCheckPoint.getValues().put(KEY, VALUE);
        stateValue.setValue(mapCheckPoint);

        List<StateValue> stateValueList = Lists.newArrayList();
        stateValueList.add(stateValue);

        String json = JSON.toJSONString(stateValueList);
        List<StateValue> stateValueList1 = JSON.parseArray(json, StateValue.class);

        MapCheckPoint mapCheckPoint1 = new MapCheckPoint();
        mapCheckPoint1.parseFromJSON((JSONObject) stateValueList1.get(0).getValue());
        Validate.isTrue(VALUE.equals(mapCheckPoint1.getValues().get(KEY)));
    }
}
