package com.github.fevernova.framework.service.checkpoint;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;


@ToString
@Getter
public class MapCheckPoint implements CheckPoint {


    private Map<String, String> values;


    public MapCheckPoint() {

        this.values = Maps.newConcurrentMap();
    }


    @Override public void parseFromJSON(JSONObject jsonObject) {

        jsonObject.getJSONObject("values").forEach((s, o) -> values.put(s, o != null ? o.toString() : null));
    }
}
