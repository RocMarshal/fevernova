package com.github.fevernova.data;


import com.github.fevernova.data.message.DataContainer;
import com.github.fevernova.data.message.Meta;
import com.github.fevernova.data.message.Opt;
import com.github.fevernova.data.message.SerializerHelper;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;

import java.util.List;


public class TestMsg {


    public static void main(String[] args) {

        int loop = 1;

        SerializerHelper serializerHelper = new SerializerHelper(2048, 32, 8192);

        List<Meta.MetaEntity> metaEntities = Lists.newArrayList();
        metaEntities.add(new Meta.MetaEntity("id", Schema.Type.LONG));
        metaEntities.add(new Meta.MetaEntity("name", Schema.Type.STRING));
        metaEntities.add(new Meta.MetaEntity("age", Schema.Type.INT));
        metaEntities.add(new Meta.MetaEntity("score", Schema.Type.LONG));
        metaEntities.add(new Meta.MetaEntity("from", Schema.Type.LONG));
        metaEntities.add(new Meta.MetaEntity("to", Schema.Type.LONG));
        metaEntities.add(new Meta.MetaEntity("attribute", Schema.Type.STRING));
        metaEntities.add(new Meta.MetaEntity("xid", Schema.Type.LONG));
        metaEntities.add(new Meta.MetaEntity("state", Schema.Type.BOOLEAN));

        for (int x = 0; x < 100; x++) {
            metaEntities.add(new Meta.MetaEntity("detail" + x, Schema.Type.STRING));
        }

        Meta meta = new Meta(1, metaEntities);

        DataContainer dataContainer = DataContainer.createDataContainer4Write(meta, Opt.INSERT);
        dataContainer.put(0, 123456789L, null);
        dataContainer.put(1, "1234567890", null);
        dataContainer.put(2, 123, null);
        dataContainer.put(3, 3221L, null);
        dataContainer.put(4, 123456789L, null);
        dataContainer.put(5, 123456789L, null);
        dataContainer.put(6, "1234567890", null);
        dataContainer.put(7, 123456789L, null);
        dataContainer.put(8, true, null);

        for (int x = 0; x < 100; x++) {
            dataContainer.put(x + 9, "12345678901234567890", null);
        }

        System.out.println(serializerHelper.serialize(null, dataContainer.writeFinished()).length);

        for (int i = 0; i < loop; i++) {
            byte[] result = serializerHelper.serialize(null, dataContainer.writeFinished());
        }

        long st = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            byte[] result = serializerHelper.serialize(null, dataContainer.writeFinished());
            DataContainer data = serializerHelper.localDeserialize(null, result);
            System.out.println(result.length);
            DataContainer dataContainer1 = DataContainer.createDataContainer4Read(meta, data.getData());
            System.out.println(dataContainer1.getData().getOpt());
            System.out.println(data.getNid());
            System.out.println(data.getSid());
            System.out.println(data.getData().getTimestamp());
        }

        long et = System.currentTimeMillis();
        System.out.println(et - st);
    }
}
