package com.github.fevernova.data.message;


import com.github.fevernova.framework.common.structure.list.FixedList;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.Validate;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Map;


public class DataContainer {


    private final static Map<Long, Meta> CACHE = Maps.newConcurrentMap();

    @Getter
    private Meta meta;

    @Getter
    private Data data;

    private BitSet updatesBitSet;

    private int positionRatio = 1;


    private DataContainer(Meta meta, Data data, Opt opt) {

        this.meta = meta;
        this.data = data;
        this.data.setOpt(opt);
        this.data.setMetaId(this.meta.getMetaId());
        this.data.setTags(Maps.newHashMap());

        if (Opt.UPDATE == this.data.getOpt()) {
            this.updatesBitSet = new BitSet(this.meta.columnSize());
            this.positionRatio = 2;
        }

        this.data.setLongs(createList(DataType.LONG));
        this.data.setStrings(createList(DataType.STRING));
        this.data.setInts(createList(DataType.INT));
        this.data.setDoubles(createList(DataType.DOUBLE));
        this.data.setFloats(createList(DataType.FLOAT));
        this.data.setBytes(createList(DataType.BYTES));
        this.data.setBooleans(createList(DataType.BOOLEAN));

        if (!CACHE.containsKey(meta.getMetaId())) {
            CACHE.put(meta.getMetaId(), meta);
        }
    }


    private DataContainer(Meta meta0, Data data) {

        this.data = data;
        if (meta0 == null) {
            Meta p = CACHE.get(data.getMetaId());
            if (p == null) {
                p = new Meta(data.getMetaId(), data.getMeta().array());
                CACHE.put(data.getMetaId(), p);
            } else {
                Validate.isTrue(p.getBytes().length == data.getMeta().array().length);
            }
            meta0 = p;
        }
        this.meta = meta0;

        if (Opt.UPDATE == this.data.getOpt()) {
            this.updatesBitSet = new BitSet(this.meta.columnSize());
            convertFromLongToBitSet();
            this.positionRatio = 2;
        }
    }


    public static DataContainer createDataContainer4Write(Meta meta, Opt opt) {

        return new DataContainer(meta, new Data(), opt);
    }


    public static DataContainer createDataContainer4Read(Meta meta, Data data) {

        return new DataContainer(meta, data);
    }


    private List createList(DataType type) {

        int size = this.meta.typeSize(type);
        return size == 0 ? null : new FixedList(size * this.positionRatio);
    }


    private void convertFromLongToBitSet() {

        long value = this.data.getUpdatesLong();
        int index = 0;
        while (value != 0L) {
            if (value % 2L != 0) {
                this.updatesBitSet.set(index);
            }
            ++index;
            value = value >>> 1;
        }
    }


    public void put(String columnName, Object val) {

        put(columnName, val, null, false);
    }


    public void put(String columnName, Object val, Object oldVal) {

        put(columnName, val, oldVal, true);
    }


    public void put(String columnName, Object val, Object oldVal, boolean change) {

        Meta.MetaEntity metaEntity = this.meta.getEntity(columnName);
        put(metaEntity, val, oldVal, change);
    }


    public void put(int index, Object val) {

        put(index, val, null, false);
    }


    public void put(int index, Object val, Object oldVal) {

        put(index, val, oldVal, true);
    }


    public void put(int index, Object val, Object oldVal, boolean change) {

        Meta.MetaEntity metaEntity = this.meta.getEntity(index);
        put(metaEntity, val, oldVal, change);
    }


    public void put(Meta.MetaEntity metaEntity, Object val) {

        put(metaEntity, val, null, false);
    }


    public void put(Meta.MetaEntity metaEntity, Object val, Object oldVal) {

        put(metaEntity, val, oldVal, true);
    }


    public void put(Meta.MetaEntity metaEntity, Object val, Object oldVal, boolean change) {

        int pos = metaEntity.getIndexOftype() * this.positionRatio;
        switch (metaEntity.getType()) {
            case STRING:
                this.data.getStrings().set(pos, (String) val);
                if (change) {
                    this.data.getStrings().set(pos + 1, (String) oldVal);
                    this.updatesBitSet.set(metaEntity.getIndexOfall());
                }
                break;
            case BYTES:
                this.data.getBytes().set(pos, (ByteBuffer) val);
                if (change) {
                    this.data.getBytes().set(pos + 1, (ByteBuffer) oldVal);
                    this.updatesBitSet.set(metaEntity.getIndexOfall());
                }
                break;
            case INT:
                this.data.getInts().set(pos, (Integer) val);
                if (change) {
                    this.data.getInts().set(pos + 1, (Integer) oldVal);
                    this.updatesBitSet.set(metaEntity.getIndexOfall());
                }
                break;
            case LONG:
                this.data.getLongs().set(pos, (Long) val);
                if (change) {
                    this.data.getLongs().set(pos + 1, (Long) oldVal);
                    this.updatesBitSet.set(metaEntity.getIndexOfall());
                }
                break;
            case FLOAT:
                this.data.getFloats().set(pos, (Float) val);
                if (change) {
                    this.data.getFloats().set(pos + 1, (Float) oldVal);
                    this.updatesBitSet.set(metaEntity.getIndexOfall());
                }
                break;
            case DOUBLE:
                this.data.getDoubles().set(pos, (Double) val);
                if (change) {
                    this.data.getDoubles().set(pos + 1, (Double) oldVal);
                    this.updatesBitSet.set(metaEntity.getIndexOfall());
                }
                break;
            case BOOLEAN:
                this.data.getBooleans().set(pos, (Boolean) val);
                if (change) {
                    this.data.getBooleans().set(pos + 1, (Boolean) oldVal);
                    this.updatesBitSet.set(metaEntity.getIndexOfall());
                }
                break;
            default:
                throw new RuntimeException("exchange.DataContainer put type error : " + metaEntity.getType());
        }
    }


    public DataContainer writeFinished() {

        if (Opt.UPDATE == this.data.getOpt()) {
            long value = 0L;
            for (int i = 0; i < this.updatesBitSet.length(); ++i) {
                value += this.updatesBitSet.get(i) ? (1L << i) : 0L;
            }
            this.data.setUpdatesLong(value);
        }
        this.data.setMeta(ByteBuffer.wrap(CACHE.get(this.data.getMetaId()).getBytes()));
        return this;
    }


    public void iterate(Callback callback) {

        for (Meta.MetaEntity metaEntity : this.meta.getEntities()) {
            boolean change = (Opt.UPDATE == this.data.getOpt() && this.updatesBitSet.get(metaEntity.getIndexOfall()));
            Object val = get(metaEntity.getType(), metaEntity.getIndexOftype());
            Object oldVal = change ? get(metaEntity.getType(), metaEntity.getIndexOftype() + 1) : null;
            callback.feed(metaEntity, change, val, oldVal);
        }
    }


    public void get(String columnName, Callback callback) {

        Meta.MetaEntity metaEntity = this.meta.getEntity(columnName);
        get(metaEntity, callback);
    }


    public void get(int index, Callback callback) {

        Meta.MetaEntity metaEntity = this.meta.getEntity(index);
        get(metaEntity, callback);
    }


    private void get(Meta.MetaEntity metaEntity, Callback callback) {

        boolean change = (this.data.getOpt() == Opt.UPDATE && this.updatesBitSet.get(metaEntity.getIndexOfall()));
        Object val = get(metaEntity.getType(), metaEntity.getIndexOftype());
        Object oldVal = change ? get(metaEntity.getType(), metaEntity.getIndexOftype() + 1) : null;
        callback.feed(metaEntity, change, val, oldVal);
    }


    private Object get(DataType type, int indexOfType) {

        switch (type) {
            case STRING:
                return this.data.getStrings().get(indexOfType);
            case BYTES:
                return this.data.getBytes().get(indexOfType);
            case INT:
                return this.data.getInts().get(indexOfType);
            case LONG:
                return this.data.getLongs().get(indexOfType);
            case FLOAT:
                return this.data.getFloats().get(indexOfType);
            case DOUBLE:
                return this.data.getDoubles().get(indexOfType);
            case BOOLEAN:
                return this.data.getBooleans().get(indexOfType);
            default:
                throw new RuntimeException("exchange.DataContainer get type error : " + type);
        }
    }


    public long getNid() {

        return this.data.getNid();
    }


    public void setNid(long nid) {

        this.data.setNid(nid);
    }


    public String getSid() {

        return this.data.getSid();
    }


    public void setSid(String sid) {

        this.data.setSid(sid);
    }


    public void putTag(String key, String val) {

        this.data.getTags().put(key, val);
    }


    public String getTag(String key) {

        return this.data.getTags().get(key);
    }


    interface Callback {


        void feed(Meta.MetaEntity metaEntity, boolean change, Object val, Object oldVal);

    }
}
