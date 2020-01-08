package com.github.fevernova.io.kafka.data;


import com.github.fevernova.framework.common.data.Data;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class KafkaData implements Data {


    private String topic;

    private String destTopic;

    private byte[] key;

    private byte[] value;

    private int partitionId = -1;

    private long timestamp = 0L;


    @Override public void clearData() {

        //this.topic = null;
        //this.destTopic = null;
        //this.key = null;
        //this.value = null;
        //this.partitionId = -1;
        //this.timestamp = 0L;
    }


    @Override public byte[] getBytes() {

        return this.value;
    }

}
