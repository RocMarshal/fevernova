package com.github.fevernova.data.message;


import com.github.fevernova.framework.common.Util;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;


public class Meta {


    @Getter
    private final long metaId;

    @Getter
    private final List<MetaEntity> entities;

    @Getter
    private final byte[] bytes;

    private final Map<String, MetaEntity> entityMap = Maps.newHashMap();

    private final int[] typeCounter = new int[DataType.values().length];


    public Meta(List<MetaEntity> entities) {

        Validate.notNull(entities);
        this.entities = entities;

        for (int i = 0; i < this.entities.size(); i++) {
            MetaEntity metaEntity = this.entities.get(i);
            metaEntity.indexOfall = i;
            metaEntity.indexOftype = this.typeCounter[metaEntity.type.index]++;
            this.entityMap.put(metaEntity.getColumnName(), metaEntity);
        }
        this.bytes = toBytes4Cache();
        this.metaId = Hashing.murmur3_128().hashBytes(this.bytes).asLong();
    }


    public Meta(long metaId, byte[] bytes) {

        this.metaId = metaId;
        this.entities = Lists.newArrayList();
        this.bytes = bytes;

        String[] strs = new String(Util.unzip(bytes)).split(";");
        for (int i = 0; i < strs.length; i++) {
            String[] kv = strs[i].split(":");
            MetaEntity metaEntity = new MetaEntity(kv[0], DataType.location(Integer.valueOf(kv[1])));
            metaEntity.indexOfall = i;
            metaEntity.indexOftype = this.typeCounter[metaEntity.type.index]++;
            this.entities.add(metaEntity);
            this.entityMap.put(metaEntity.getColumnName(), metaEntity);
        }
    }


    public int columnSize() {

        return this.entities.size();
    }


    public MetaEntity getEntity(String columnName) {

        return this.entityMap.get(columnName);
    }


    public MetaEntity getEntity(int index) {

        return this.entities.get(index);
    }


    public int typeSize(DataType type) {

        return this.typeCounter[type.index];
    }


    private byte[] toBytes4Cache() {

        StringBuilder sb = new StringBuilder();
        for (MetaEntity entity : this.entities) {
            sb.append(entity.getColumnName()).append(":").append(entity.getType().ordinal()).append(";");
        }
        return Util.zip(sb);
    }


    @Getter
    @ToString
    public static class MetaEntity {


        private String columnName;

        private DataType type;

        private int indexOfall;

        private int indexOftype;


        public MetaEntity(String columnName, DataType type) {

            this.columnName = columnName;
            this.type = type;
        }
    }

}
