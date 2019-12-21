package com.github.fevernova.kafka.data;


import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.service.checkpoint.CheckPoint;
import com.google.common.collect.Maps;
import lombok.*;

import java.util.Map;


@Setter
@Getter
@Builder
@ToString
@AllArgsConstructor
public class KafkaCheckPoint implements CheckPoint {


    private Map<String, Map<Integer, Long>> offsets;


    public KafkaCheckPoint() {

        this.offsets = Maps.newHashMap();
    }


    public void put(String topic, int partition, long offset) {

        if (!this.offsets.containsKey(topic)) {
            this.offsets.put(topic, Maps.newHashMap());
        }
        this.offsets.get(topic).put(partition, offset);
    }


    @Override public void parseFromJSON(JSONObject jsonObject) {

        jsonObject.forEach((s, o) -> {

            Map<Integer, Long> t = Maps.newHashMap();
            ((JSONObject) o).forEach((s1, o1) -> t.put(Integer.valueOf(s1), Long.valueOf(o1.toString())));
            offsets.put(s, t);
        });
    }
}
