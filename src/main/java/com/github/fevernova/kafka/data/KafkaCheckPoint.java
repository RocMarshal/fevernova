package com.github.fevernova.kafka.data;


import com.github.fevernova.framework.service.checkpoint.CheckPoint;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;


public class KafkaCheckPoint implements CheckPoint {

    //TODO 要支持多topic

    @Getter
    private Map<Integer, Long> offsets;


    public KafkaCheckPoint() {

        this.offsets = Maps.newHashMap();
    }


    public void put(int partition, long offset) {

        this.offsets.put(partition, offset);
    }

}
