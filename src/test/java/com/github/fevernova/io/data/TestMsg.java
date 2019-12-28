package com.github.fevernova.io.data;


import com.github.fevernova.io.data.message.*;
import com.github.fevernova.framework.common.Util;
import com.google.common.collect.Lists;

import java.util.List;


public class TestMsg {


    public static void main(String[] args) {

        int loop = 1000_000;

        SerializerHelper serializerHelper = new SerializerHelper(2048, 32, 8192);

        List<Meta.MetaEntity> metaEntities = Lists.newArrayList();
        metaEntities.add(new Meta.MetaEntity("id", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("name", DataType.STRING));
        metaEntities.add(new Meta.MetaEntity("age", DataType.INT));
        metaEntities.add(new Meta.MetaEntity("score", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("from", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("to", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("attribute", DataType.STRING));
        metaEntities.add(new Meta.MetaEntity("xid", DataType.LONG));
        metaEntities.add(new Meta.MetaEntity("state", DataType.BOOLEAN));

        for (int x = 0; x < 50; x++) {
            metaEntities.add(new Meta.MetaEntity("detail" + x, DataType.LONG));
        }

        Meta meta = new Meta(metaEntities);

        DataContainer dataContainer = DataContainer.createDataContainer4Write(meta, Opt.INSERT);
        dataContainer.put(0, 123456789L);
        dataContainer.put(1, "1234567890");
        dataContainer.put(2, 123);
        dataContainer.put(3, 3221L);
        dataContainer.put(4, 123456789L);
        dataContainer.put(5, 123456789L);
        dataContainer.put(6, "1234567890");
        dataContainer.put(7, 123456789L);
        dataContainer.put(8, true);

        for (int x = 0; x < 50; x++) {
            dataContainer.put(x + 9, 1234567890123456789L);
        }

        byte[] sresult = serializerHelper.serialize(null, dataContainer.writeFinished());
        System.out.println(meta.getBytes().length);
        System.out.println(sresult.length);


        long st = Util.nowMS();
        for (int i = 0; i < loop; i++) {
            //DataContainer dresult = serializerHelper.deserialize(null, sresult);
            byte[] result = serializerHelper.serialize(null, dataContainer);
        }

        long et = Util.nowMS();
        System.out.println(et - st);
    }
}
