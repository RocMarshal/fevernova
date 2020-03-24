package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.task.exchange.data.order.OrderAction;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.nio.ByteBuffer;


@Getter
@Setter
@ToString
public class MatchPart {


    private long matchPrice;

    private long matchSize;

    private OrderAction driverAction;


    public void from(ByteBuffer byteBuffer) {

        this.matchPrice = byteBuffer.getLong();
        this.matchSize = byteBuffer.getLong();
        this.driverAction = OrderAction.of(byteBuffer.get());
    }


    public void getBytes(ByteBuffer byteBuffer) {

        byteBuffer.putLong(this.matchPrice);
        byteBuffer.putLong(this.matchSize);
        byteBuffer.put(this.driverAction.code);
    }


    public void clearData() {

        this.matchPrice = -1L;
        this.matchSize = 0L;
        this.driverAction = null;
    }
}
