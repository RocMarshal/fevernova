package com.github.fevernova.task;


import com.github.fevernova.Common;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.io.data.message.*;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;


public class T_CMD {


    @Test
    public void T_cmdS() {

        List<Meta.MetaEntity> metaEntities = Lists.newArrayList();
        metaEntities.add(new Meta.MetaEntity("orderCommandType", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("orderId", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("symbolId", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("userId", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("timestamp", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("orderAction", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("orderType", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("price", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("size", DataType.LONG));
        Meta meta = new Meta(metaEntities);

        DataContainer dataContainer = DataContainer.createDataContainer4Write(meta, Opt.INSERT);
        dataContainer.put(0, 0);
        dataContainer.put(1, 1234567890L);
        dataContainer.put(2, 1);
        dataContainer.put(3, 1234567890L);
        dataContainer.put(4, 1234567890L);
        dataContainer.put(5, 1);
        dataContainer.put(6, 0);
        dataContainer.put(7, 1000L);
        dataContainer.put(8, 10000L);

        SerializerHelper serializerHelper = new SerializerHelper();
        byte[] bytes = serializerHelper.serialize(null, dataContainer.writeFinished());

        Common.warn();

        long st = Util.nowMS();
        for (int i = 0; i < 1_0000_000; i++) {
            serializerHelper.deserialize(null, bytes);
            OrderCommand data = new OrderCommand();
            dataContainer.iterate((metaEntity, change, val, oldVal) -> {

                switch (metaEntity.getColumnName()) {
                    case "orderCommandType":
                        data.setOrderCommandType(OrderCommandType.of((int) val));
                        break;
                    case "orderId":
                        data.setOrderId((long) val);
                        break;
                    case "symbolId":
                        data.setSymbolId((int) val);
                        break;
                    case "userId":
                        data.setUserId((long) val);
                        break;
                    case "timestamp":
                        data.setTimestamp((long) val);
                        break;
                    case "orderAction":
                        data.setOrderAction(OrderAction.of((int) val));
                        break;
                    case "orderType":
                        data.setOrderType(OrderType.of((int) val));
                        break;
                    case "price":
                        data.setPrice((long) val);
                        break;
                    case "size":
                        data.setSize((long) val);
                        break;
                }
            });
        }

        long et = Util.nowMS();
        System.out.println(et - st);
    }


    @Test
    public void T_cmdP() {

        List<Meta.MetaEntity> metaEntities = Lists.newArrayList();
        metaEntities.add(new Meta.MetaEntity("orderCommandType", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("orderId", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("symbolId", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("userId", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("timestamp", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("orderAction", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("orderType", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("price", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("size", DataType.LONG));
        Meta meta = new Meta(metaEntities);

        DataContainer dataContainer = DataContainer.createDataContainer4Write(meta, Opt.INSERT);
        dataContainer.put(0, 0);
        dataContainer.put(1, 1234567890L);
        dataContainer.put(2, 1);
        dataContainer.put(3, 1234567890L);
        dataContainer.put(4, 1234567890L);
        dataContainer.put(5, 1);
        dataContainer.put(6, 0);
        dataContainer.put(7, 1000L);
        dataContainer.put(8, 10000L);

        SerializerHelper serializerHelper = new SerializerHelper();
        byte[] bytes = serializerHelper.serialize(null, dataContainer.writeFinished());

        Common.warn();

        long st = Util.nowMS();
        for (int i = 0; i < 1_0000_000; i++) {
            serializerHelper.deserialize(null, bytes);
        }
        long et = Util.nowMS();
        System.out.println(et - st);
    }


    @Test
    public void T_binaryCMD() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(47);
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(1234567890L);
        byteBuffer.putInt(1);
        byteBuffer.putLong(1234567890L);
        byteBuffer.putLong(1234567890L);
        byteBuffer.put((byte) 1);
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(1000L);
        byteBuffer.putLong(10000L);
        byte[] r = byteBuffer.array();

        Common.warn();
        long st = Util.nowMS();
        long x = 0;
        for (int i = 0; i < 1_0000_0000; i++) {
            byte[] m = new byte[47];
            System.arraycopy(r, 0, m, 0, 47);
            ByteBuffer dx = ByteBuffer.wrap(m);
            OrderCommand data = new OrderCommand();
            data.setOrderCommandType(OrderCommandType.of(dx.get()));
            data.setOrderId(dx.getLong());
            data.setSymbolId(dx.getInt());
            data.setUserId(dx.getLong());
            data.setTimestamp(dx.getLong());
            data.setOrderAction(OrderAction.of(dx.get()));
            data.setOrderType(OrderType.of(dx.get()));
            data.setPrice(dx.getLong());
            data.setSize(dx.getLong());
            x += data.getSize();
        }
        System.out.println(x);
        long et = Util.nowMS();
        System.out.println(et - st);
    }
}
