package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.FNException;
import com.github.fevernova.framework.common.context.TaskContext;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.OutputStream;
import java.util.Locale;


@Slf4j
public class DataSerializerFactory {


    public static DataSerializer getInstance(String serializerType, TaskContext context, OutputStream out) {

        Preconditions.checkNotNull(serializerType, "serializer type must not be null");

        // try to find builder class in enum of known output serializers
        DataSerializerType type;
        try {
            type = DataSerializerType.valueOf(serializerType.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            log.debug("Not in enum, loading builder class: {}", serializerType);
            type = DataSerializerType.OTHER;
        }
        Class<? extends DataSerializer.Builder> builderClass = type.getBuilderClass();

        // handle the case where they have specified their own builder in the config
        if (builderClass == null) {
            try {
                Class c = Class.forName(serializerType);
                if (c != null && DataSerializer.Builder.class.isAssignableFrom(c)) {
                    builderClass = (Class<? extends DataSerializer.Builder>) c;
                } else {
                    String errMessage = "Unable to instantiate Builder from " + serializerType + ": does not appear to " +
                                        "implement " + DataSerializer.Builder.class.getName();
                    throw new FNException(errMessage);
                }
            } catch (ClassNotFoundException ex) {
                log.error("Class not found: " + serializerType, ex);
                throw new FNException(ex);
            }
        }

        // build the builder
        DataSerializer.Builder builder;
        try {
            builder = builderClass.newInstance();
        } catch (InstantiationException ex) {
            String errMessage = "Cannot instantiate builder: " + serializerType;
            log.error(errMessage, ex);
            throw new FNException(errMessage, ex);
        } catch (IllegalAccessException ex) {
            String errMessage = "Cannot instantiate builder: " + serializerType;
            log.error(errMessage, ex);
            throw new FNException(errMessage, ex);
        }

        return builder.build(context, out);
    }
}
