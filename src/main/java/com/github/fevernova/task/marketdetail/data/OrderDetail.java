package com.github.fevernova.task.marketdetail.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.Validate;

import java.nio.ByteBuffer;


@Getter
@Setter
@ToString
public class OrderDetail implements Data {


    private int symbolId;

    private long timestamp;

    private OrderAction driverAction;

    private long matchPrice;

    private long matchSize;


    public void from(OrderMatch orderMatch) {

        this.symbolId = orderMatch.getSymbolId();
        this.timestamp = orderMatch.getTimestamp();
        this.driverAction = orderMatch.getMatchPart().getDriverAction();
        this.matchPrice = orderMatch.getMatchPart().getMatchPrice();
        this.matchSize = orderMatch.getMatchPart().getMatchSize();
    }


    public void from(byte[] bytes) {

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byte version = byteBuffer.get();
        Validate.isTrue(version == 0);
        this.symbolId = byteBuffer.getInt();
        this.timestamp = byteBuffer.getLong();
        this.driverAction = OrderAction.of(byteBuffer.get());
        this.matchPrice = byteBuffer.getLong();
        this.matchSize = byteBuffer.getLong();
    }


    @Override public byte[] getBytes() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(30);
        byteBuffer.put((byte) 0);
        byteBuffer.putInt(this.symbolId);
        byteBuffer.putLong(this.timestamp);
        byteBuffer.put(this.driverAction.code);
        byteBuffer.putLong(this.matchPrice);
        byteBuffer.putLong(this.matchSize);
        return byteBuffer.array();
    }

}
