package com.github.fevernova.data.message;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;


public class SerializerHelper implements Deserializer<DataContainer>, Serializer<DataContainer> {


    private DatumWriter<Data> writer = new SpecificDatumWriter<>(Data.getClassSchema());

    private BinaryEncoder binaryEncoder;

    private int encoderBufferSize;

    private EncoderFactory encoderFactory = new EncoderFactory();

    private int byteArrayBaseSize;

    private DatumReader<Data> reader = new SpecificDatumReader<>(Data.getClassSchema());

    private BinaryDecoder binaryDecoder;

    private int decoderBufferSize;

    private DecoderFactory decoderFactory = new DecoderFactory();


    public SerializerHelper() {

        this(2048, 1024, 4096);
    }


    public SerializerHelper(int encoderBufferSize, int byteArrayBaseSize, int decoderBufferSize) {

        this.encoderBufferSize = encoderBufferSize;
        this.byteArrayBaseSize = byteArrayBaseSize;
        this.encoderFactory.configureBufferSize(this.encoderBufferSize);
        this.decoderBufferSize = decoderBufferSize;
        this.decoderFactory.configureDecoderBufferSize(this.decoderBufferSize);
    }


    @Override
    public byte[] serialize(String s, DataContainer data) {

        try (ByteArrayOutputStream os = new ByteArrayOutputStream(this.byteArrayBaseSize)) {
            this.binaryEncoder = this.encoderFactory.binaryEncoder(os, this.binaryEncoder);
            this.writer.write(data.getData(), this.binaryEncoder);
            this.binaryEncoder.flush();
            return os.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public byte[] serializeJSON(String s, DataContainer data) {

        JSONObject json = new JSONObject();
        json.put("opt", data.getData().getOpt().name());
        json.put("timestamp", data.getData().getTimestamp());
        json.put("tags", data.getData().getTags());
        final Map<String, Object> cur = Maps.newHashMapWithExpectedSize(data.getMeta().columnSize());
        final Map<String, Object> pre = Maps.newHashMap();
        data.iterate((metaEntity, change, val, oldVal) -> {
            cur.put(metaEntity.getColumnName(), val);
            if (change) {
                pre.put(metaEntity.getColumnName(), oldVal);
            }
        });
        json.put("cur", cur);
        json.put("pre", pre);
        return json.toJSONString().getBytes();
    }


    @Override
    public DataContainer deserialize(String s, byte[] bytes) {

        this.binaryDecoder = this.decoderFactory.binaryDecoder(bytes, this.binaryDecoder);
        try {
            Data r = this.reader.read(null, this.binaryDecoder);
            return DataContainer.createDataContainer4Read(null, r);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }


    @Override
    public void close() {

    }
}
