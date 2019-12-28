package com.github.fevernova.io.kafka;


import com.github.fevernova.framework.common.context.TaskContext;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


public class KafkaUtil {


    public synchronized static KafkaConsumer<byte[], byte[]> createConsumer(TaskContext kafkaContext) {

        return new KafkaConsumer<>(getConsumerProperties(kafkaContext));
    }


    public synchronized static KafkaProducer<byte[], byte[]> createProducer(TaskContext kafkaContext) {

        return new KafkaProducer<>(getProducerProperties(kafkaContext));
    }


    private static Properties getConsumerProperties(TaskContext kafkaContext) {

        Properties props = new Properties();
        props.putAll(kafkaContext.getParameters());

        Validate.notBlank(props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Validate.notBlank(props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
        Validate.notBlank(props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putIfAbsent(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "2097152");
        props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100000");
        props.putIfAbsent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10240");
        props.putIfAbsent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "3000");
        props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

        return props;
    }


    private static Properties getProducerProperties(TaskContext kafkaContext) {

        Properties props = new Properties();
        props.putAll(kafkaContext.getParameters());

        Validate.notBlank(props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        Validate.notBlank(props.getProperty(ProducerConfig.CLIENT_ID_CONFIG));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        props.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG, "10");
        props.putIfAbsent(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        props.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.putIfAbsent(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");
        props.putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
        props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.putIfAbsent(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "20971520");

        return props;
    }

}
