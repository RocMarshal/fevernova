package com.github.fevernova.io.hdfs.serialization;


public enum DataSerializerType {

    TEXT(BodyTextEventSerializer.Builder.class), AVRO_EVENT(AvroEventSerializer.Builder.class), OTHER(null);

    private final Class<? extends DataSerializer.Builder> builderClass;


    DataSerializerType(Class<? extends DataSerializer.Builder> builderClass) {

        this.builderClass = builderClass;
    }


    public Class<? extends DataSerializer.Builder> getBuilderClass() {

        return builderClass;
    }

}

