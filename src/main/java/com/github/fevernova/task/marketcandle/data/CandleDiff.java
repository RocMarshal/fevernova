package com.github.fevernova.task.marketcandle.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.task.exchange.data.candle.Point;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;


@Getter
@Setter
@ToString
public class CandleDiff implements Data {


    private int symbolId;

    private List<Point> diff;


    @Override public void clearData() {

        this.diff = null;
    }


    @Override public long getTimestamp() {

        return 0;
    }


    @Override public byte[] getBytes() {

        return new byte[0];
    }

}
