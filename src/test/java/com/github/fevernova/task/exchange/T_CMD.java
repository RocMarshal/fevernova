package com.github.fevernova.task.exchange;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderMode;
import com.github.fevernova.task.exchange.data.order.OrderType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class T_CMD {


    private final OrderCommand cmd = new OrderCommand();


    @Before
    public void init() {

        cmd.setOrderCommandType(OrderCommandType.PLACE_ORDER);
        cmd.setOrderId(1234567890L);
        cmd.setSymbolId(999);
        cmd.setUserId(123L);
        cmd.setTimestamp(Util.nowMS());
        cmd.setOrderAction(OrderAction.BID);
        cmd.setOrderType(OrderType.GTC);
        cmd.setOrderMode(OrderMode.SIMPLE);
        cmd.setPrice(1000L);
        cmd.setTriggerPrice(0L);
        cmd.setSize(10000L);
    }


    @Test
    public void T_CMD_To_Binary() {

        Assert.assertEquals(cmd.to().length, OrderCommand.BYTE_SIZE);
    }


    @Test
    public void T_CMD_From_Binary() {

        byte[] bytes = cmd.to();
        OrderCommand cmd2 = new OrderCommand();
        cmd2.from(bytes);
        Assert.assertEquals(cmd.toString(), cmd2.toString());
    }
}
