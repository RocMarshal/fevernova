package com.github.fevernova.task.exchange;


import com.github.fevernova.Common;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.service.state.BinaryFileIdentity;
import com.github.fevernova.framework.service.state.storage.FSStorage;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderMode;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.engine.OrderBooksEngine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;


public class T_EngineState extends T_Engine {


    private FSStorage fsStorage;

    private BinaryFileIdentity identity;


    @Before
    public void init() {

        GlobalContext globalContext = Common.createGlobalContext();
        TaskContext taskContext = Common.createTaskContext();
        orderBooksEngine = new OrderBooksEngine(globalContext, taskContext);
        provider = new TestProvider(false);
        fsStorage = new FSStorage(globalContext, taskContext);
        identity = BinaryFileIdentity.builder().componentType(ComponentType.PARSER).total(3).index(1).identity(OrderBooksEngine.CONS_NAME).build();
    }


    @Test
    public void T_snapshot() {

        OrderCommand bidCMD = buildCMD(OrderCommandType.PLACE_ORDER, OrderAction.BID, OrderType.GTC, OrderMode.SIMPLE);
        bidCMD.setPrice(1000000);
        bidCMD.setSize(100);

        OrderCommand askCMD = buildCMD(OrderCommandType.PLACE_ORDER, OrderAction.ASK, OrderType.GTC, OrderMode.SIMPLE);
        askCMD.setPrice(10000000);
        askCMD.setSize(100);

        for (int i = 0; i < 100000; i++) {
            bidCMD.setOrderId(bidCMD.getOrderId() + 2);
            askCMD.setOrderId(askCMD.getOrderId() + 2);
            bidCMD.setPrice(bidCMD.getPrice() - 1);
            askCMD.setPrice(askCMD.getPrice() + 1);
            parser(bidCMD);
            parser(askCMD);
        }

        String fp = "/tmp/fevernova/testtype-testid/3-0/data/parser_3_1_OrderBooksEngine_0_1.bin";
        File binFile = Paths.get(fp).toFile();
        if (binFile.exists()) {
            binFile.delete();
        }

        fsStorage.saveBinary(identity, new BarrierData(1L, 0L), orderBooksEngine);
        Assert.assertTrue(binFile.exists());

        orderBooksEngine = new OrderBooksEngine(null, null);
        fsStorage.recoveryBinary(fp, orderBooksEngine);

        check(100000, 100000, 200000, null);
    }
}
