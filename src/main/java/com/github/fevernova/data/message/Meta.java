package com.github.fevernova.data.message;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.Validate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class Meta {


    @Getter
    private long metaId;

    @Getter
    private final List<MetaEntity> entities;

    private final Map<String, MetaEntity> entityMap = Maps.newHashMap();

    private int[] typeCounter = new int[DataType.values().length];


    public Meta(List<MetaEntity> entities) {

        Validate.notNull(entities);
        this.entities = entities;

        for (int i = 0; i < this.entities.size(); i++) {
            MetaEntity metaEntity = this.entities.get(i);
            this.entityMap.put(metaEntity.getColumnName(), metaEntity);
            metaEntity.indexOfall = i;
            metaEntity.indexOftype = this.typeCounter[metaEntity.type.index]++;
        }
        this.metaId = Hashing.murmur3_128().hashBytes(toBytes4Cache()).asLong();
    }


    public Meta(long metaId, byte[] bytes) {

        this.metaId = metaId;
        this.entities = Lists.newArrayList();

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            GZIPInputStream ungzip = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
                bytes = out.toByteArray();
            }
            ungzip.close();
        } catch (Exception e) {
        }

        String[] strs = new String(bytes).split(";");
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


    public byte[] toBytes4Cache() {

        StringBuilder sb = new StringBuilder();
        for (MetaEntity entity : this.entities) {
            sb.append(entity.getColumnName()).append(":").append(entity.getType().ordinal()).append(";");
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(sb.toString().getBytes());
            gzip.close();
            return out.toByteArray();
        } catch (Exception e) {
        }
        return null;
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
