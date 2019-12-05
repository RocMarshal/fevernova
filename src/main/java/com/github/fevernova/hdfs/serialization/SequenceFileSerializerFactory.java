package com.github.fevernova.hdfs.serialization;


import com.github.fevernova.framework.common.context.TaskContext;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SequenceFileSerializerFactory {


    /**
     * {@link TaskContext} prefix
     */
    public static final String CTX_PREFIX = "writeFormat.";


    @SuppressWarnings("unchecked")
    public static SequenceFileSerializer getSerializer(String formatType, TaskContext context) {

        Preconditions.checkNotNull(formatType, "serialize type must not be null");

        // try to find builder class in enum of known formatters
        SequenceFileSerializerType type;
        try {
            type = SequenceFileSerializerType.valueOf(formatType);
        } catch (IllegalArgumentException e) {
            log.debug("Not in enum, loading builder class: {}", formatType);
            type = SequenceFileSerializerType.Other;
        }
        Class<? extends SequenceFileSerializer.Builder> builderClass = type.getBuilderClass();

        // handle the case where they have specified their own builder in the config
        if (builderClass == null) {
            try {
                Class c = Class.forName(formatType);
                if (c != null && SequenceFileSerializer.Builder.class.isAssignableFrom(c)) {
                    builderClass = (Class<? extends SequenceFileSerializer.Builder>) c;
                } else {
                    log.error("Unable to instantiate Builder from {}", formatType);
                    return null;
                }
            } catch (ClassNotFoundException ex) {
                log.error("Class not found: " + formatType, ex);
                return null;
            } catch (ClassCastException ex) {
                log.error("Class does not extend " + SequenceFileSerializer.Builder.class.getCanonicalName() + ": " +
                          formatType, ex);
                return null;
            }
        }

        // build the builder
        SequenceFileSerializer.Builder builder;
        try {
            builder = builderClass.newInstance();
        } catch (InstantiationException ex) {
            log.error("Cannot instantiate builder: " + formatType, ex);
            return null;
        } catch (IllegalAccessException ex) {
            log.error("Cannot instantiate builder: " + formatType, ex);
            return null;
        }

        return builder.build(context);
    }

}
