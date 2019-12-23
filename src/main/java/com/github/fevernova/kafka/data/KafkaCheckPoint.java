package com.github.fevernova.kafka.data;


import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.service.checkpoint.CheckPoint;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;


@Getter
@ToString
public class KafkaCheckPoint implements CheckPoint {


    private Map<String, Map<Integer, Long>> offsets;


    public KafkaCheckPoint() {

        this.offsets = Maps.newHashMap();
    }


    public void put(String topic, int partition, long offset) {

        Map<Integer, Long> offs = this.offsets.get(topic);
        if (offs == null) {
            offs = Maps.newHashMap();
            this.offsets.put(topic, offs);
        }
        offs.put(partition, offset);
    }


    @Override public void parseFromJSON(JSONObject jsonObject) {

        jsonObject.forEach((s, o) -> {

            Map<Integer, Long> t = Maps.newHashMap();
            ((JSONObject) o).forEach((s1, o1) -> t.put(Integer.valueOf(s1), Long.valueOf(o1.toString())));
            offsets.put(s, t);
        });
    }
}
