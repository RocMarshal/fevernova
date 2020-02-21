package com.github.fevernova.task.exchange;


import com.github.fevernova.Common;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import org.junit.Before;
import org.junit.Test;


public class T_CMD {


    private final OrderCommand cmd = new OrderCommand();

    private final int loop = 1_0000_0000;


    @Before
    public void init() {

        cmd.setOrderCommandType(OrderCommandType.PLACE_ORDER);
        cmd.setOrderId(1234567890L);
        cmd.setSymbolId(999);
        cmd.setUserId(123L);
        cmd.setTimestamp(Util.nowMS());
        cmd.setOrderAction(OrderAction.BID);
        cmd.setOrderType(OrderType.GTC);
        cmd.setPrice(1000L);
        cmd.setSize(10000L);
    }


    @Test
    public void T_CMD_To_Binary() {

        Common.warmup();
        long st = Util.nowMS();
        long x = 0L;
        for (int i = 0; i < loop; i++) {
            x += cmd.to().length;
        }
        long et = Util.nowMS();
        System.out.println(x);
        System.out.println(et - st);
    }


    @Test
    public void T_CMD_From_Binary() {

        byte[] bytes = cmd.to();

        Common.warmup();
        long st = Util.nowMS();
        long x = 0L;
        for (int i = 0; i < 1_0000_0000; i++) {
            OrderCommand data = new OrderCommand();
            data.from(bytes);
            x += data.getPrice();
        }
        long et = Util.nowMS();
        System.out.println(x);
        System.out.println(et - st);
    }
}
