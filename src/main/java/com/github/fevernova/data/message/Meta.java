package com.github.fevernova.data.message;


import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;


public class Meta {


    @Getter
    private final List<MetaEntity> entities;

    private final Map<String, MetaEntity> entityMap = Maps.newHashMap();

    @Getter
    private long metaId;

    private int typeStringsCounter = 0;

    private int typeBytesCounter = 0;

    private int typeIntsCounter = 0;

    private int typeLongsCounter = 0;

    private int typeFloatsCounter = 0;

    private int typeDoublesCounter = 0;

    private int typeBooleansCounter = 0;


    public Meta(long metaId, List<MetaEntity> entities) {

        this.metaId = metaId;
        Validate.notNull(entities);
        this.entities = entities;

        for (int i = 0; i < this.entities.size(); i++) {
            MetaEntity metaEntity = this.entities.get(i);
            this.entityMap.put(metaEntity.getColumnName(), metaEntity);
            metaEntity.indexOfall = i;
            switch (metaEntity.type) {
                case STRING:
                    metaEntity.indexOftype = this.typeStringsCounter++;
                    break;
                case BYTES:
                    metaEntity.indexOftype = this.typeBytesCounter++;
                    break;
                case INT:
                    metaEntity.indexOftype = this.typeIntsCounter++;
                    break;
                case LONG:
                    metaEntity.indexOftype = this.typeLongsCounter++;
                    break;
                case FLOAT:
                    metaEntity.indexOftype = this.typeFloatsCounter++;
                    break;
                case DOUBLE:
                    metaEntity.indexOftype = this.typeDoublesCounter++;
                    break;
                case BOOLEAN:
                    metaEntity.indexOftype = this.typeBooleansCounter++;
                    break;
                default:
                    throw new RuntimeException("exchange.Meta init error : " + metaEntity.getType());
            }
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


    public int typeSize(Schema.Type type) {

        switch (type) {
            case STRING:
                return this.typeStringsCounter;
            case BYTES:
                return this.typeBytesCounter;
            case INT:
                return this.typeIntsCounter;
            case LONG:
                return this.typeLongsCounter;
            case FLOAT:
                return this.typeFloatsCounter;
            case DOUBLE:
                return this.typeDoublesCounter;
            case BOOLEAN:
                return this.typeBooleansCounter;
            default:
                throw new RuntimeException("exchange.Meta typeSize error : " + type);
        }
    }


    @Getter
    public static class MetaEntity {


        private String columnName;

        private Schema.Type type;

        private int indexOfall;

        private int indexOftype;


        public MetaEntity(String columnName, Schema.Type type) {

            this.columnName = columnName;
            this.type = type;
        }
    }

}
