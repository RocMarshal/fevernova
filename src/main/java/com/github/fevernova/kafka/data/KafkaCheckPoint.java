package com.github.fevernova.kafka.data;


import com.github.fevernova.framework.service.checkpoint.CheckPoint;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;


public class KafkaCheckPoint implements CheckPoint {


    @Getter
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

}
