package com.github.fevernova.framework.common.data;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;


@Getter
@ToString
@AllArgsConstructor
public class BarrierData implements Data {


    private final long barrierId;

    private final long timestamp;


    @Override public byte[] getBytes() {

        return null;
    }
}
