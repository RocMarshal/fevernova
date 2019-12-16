package com.github.shyiko.mysql.binlog.event.deserialization;


import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Map;


public abstract class AbstractRowsEventDataDeserializerV2<T extends EventData> extends AbstractRowsEventDataDeserializer<T> {


    private boolean deserializeDateAndTimeAsLong;

    private boolean microsecondsPrecision;

    private Long invalidDateAndTimeRepresentation;


    public AbstractRowsEventDataDeserializerV2(Map<Long, TableMapEventData> tableMapEventByTableId) {

        super(tableMapEventByTableId);
    }


    @Override protected Serializable deserializeBit(int meta, ByteArrayInputStream inputStream) throws IOException {

        BitSet bs = (BitSet) super.deserializeBit(meta, inputStream);
        Long value = 0L;
        for (int i = 0; i < bs.length(); ++i) {
            value += bs.get(i) ? (1L << i) : 0L;
        }
        return value;
    }


    @Override protected Serializable deserializeNewDecimal(int meta, ByteArrayInputStream inputStream) throws IOException {

        return super.deserializeNewDecimal(meta, inputStream).toString();
    }


    @Override protected Serializable deserializeDate(ByteArrayInputStream inputStream) throws IOException {

        int value = inputStream.readInteger(3);
        int day = value % 32;
        value >>>= 5;
        int month = value % 16;
        int year = value >> 4;

        if (deserializeDateAndTimeAsLong) {
            Long timestamp = asUnixTime(year, month, day, 0, 0, 0, 0);
            return castTimestamp(timestamp, 0);
        }

        StringBuffer apd = new StringBuffer();
        appendYearTime(apd, year, "");
        appendDateTime(apd, month, "-");
        appendDateTime(apd, day, "-");
        return apd.toString();
    }


    @Override protected Serializable deserializeDatetime(ByteArrayInputStream inputStream) throws IOException {

        int[] split = split(inputStream.readLong(8), 100, 6);
        if (deserializeDateAndTimeAsLong) {
            Long timestamp = asUnixTime(split[5], split[4], split[3], split[2], split[1], split[0], 0);
            return castTimestamp(timestamp, 0);
        }

        StringBuffer apd = new StringBuffer();
        appendYearTime(apd, split[5], "");
        appendDateTime(apd, split[4], "-");
        appendDateTime(apd, split[3], "-");
        appendDateTime(apd, split[2], " ");
        appendDateTime(apd, split[1], ":");
        appendDateTime(apd, split[0], ":");
        return apd.toString();
    }


    @Override protected Serializable deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {

        long datetime = bigEndianLong(inputStream.read(5), 0, 5);
        int yearMonth = bitSlice(datetime, 1, 17, 40);
        int fsp = deserializeFractionalSeconds(meta, inputStream);
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = bitSlice(datetime, 18, 5, 40);
        int hour = bitSlice(datetime, 23, 5, 40);
        int minute = bitSlice(datetime, 28, 6, 40);
        int second = bitSlice(datetime, 34, 6, 40);

        if (deserializeDateAndTimeAsLong) {
            Long timestamp = asUnixTime(year, month, day, hour, minute, second, fsp / 1000);
            return castTimestamp(timestamp, fsp);
        }
        StringBuffer apd = new StringBuffer();
        appendYearTime(apd, year, "");
        appendDateTime(apd, month, "-");
        appendDateTime(apd, day, "-");
        appendDateTime(apd, hour, " ");
        appendDateTime(apd, minute, ":");
        appendDateTime(apd, second, ":");
        appendMillisecond(apd, fsp / 1000);
        return apd.toString();
    }


    @Override protected Serializable deserializeTime(ByteArrayInputStream inputStream) throws IOException {

        int value = inputStream.readInteger(3);
        int[] split = split(value, 100, 3);
        if (deserializeDateAndTimeAsLong) {
            Long timestamp = asUnixTime(1970, 1, 1, split[2], split[1], split[0], 0);
            return castTimestamp(timestamp, 0);
        }
        StringBuffer apd = new StringBuffer();
        appendDateTime(apd, split[2], "");
        appendDateTime(apd, split[1], ":");
        appendDateTime(apd, split[0], ":");
        return apd.toString();
    }


    @Override protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {

        long time = bigEndianLong(inputStream.read(3), 0, 3);
        int fsp = deserializeFractionalSeconds(meta, inputStream);

        int hour = bitSlice(time, 2, 10, 24);
        int minute = bitSlice(time, 12, 6, 24);
        int second = bitSlice(time, 18, 6, 24);

        if (deserializeDateAndTimeAsLong) {
            Long timestamp = asUnixTime(1970, 1, 1, hour, minute, second, fsp / 1000);
            return castTimestamp(timestamp, fsp);
        }

        StringBuffer apd = new StringBuffer();
        appendDateTime(apd, hour, "");
        appendDateTime(apd, minute, ":");
        appendDateTime(apd, second, ":");
        appendMillisecond(apd, fsp / 1000);
        return apd.toString();
    }


    private static void appendYearTime(StringBuffer stringBuffer, int value, String split) {

        stringBuffer.append(split);
        if (value > 999) {

        } else if (value > 99) {
            stringBuffer.append("0");
        } else if (value > 9) {
            stringBuffer.append("00");
        } else {
            stringBuffer.append("000");
        }
        stringBuffer.append(value);
    }


    private static void appendDateTime(StringBuffer stringBuffer, int value, String split) {

        stringBuffer.append(split);
        if (value < 10)
            stringBuffer.append("0");
        stringBuffer.append(value);
    }


    private static void appendMillisecond(StringBuffer stringBuffer, int value) {

        stringBuffer.append(".");
        if (value < 10)
            stringBuffer.append("00");
        else if (value < 100)
            stringBuffer.append("0");
        stringBuffer.append(value);
    }


    @Override
    void setDeserializeDateAndTimeAsLong(boolean value) {

        this.deserializeDateAndTimeAsLong = value;
        super.setDeserializeDateAndTimeAsLong(value);
    }


    @Override
    public void setMicrosecondsPrecision(boolean value) {

        this.microsecondsPrecision = value;
        super.setMicrosecondsPrecision(value);
    }


    void setInvalidDateAndTimeRepresentation(Long value) {

        this.invalidDateAndTimeRepresentation = value;
        super.setInvalidDateAndTimeRepresentation(value);
    }


    private static long bigEndianLong(byte[] bytes, int offset, int length) {

        long result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }


    private static int bitSlice(long value, int bitOffset, int numberOfBits, int payloadSize) {

        long result = value >> payloadSize - (bitOffset + numberOfBits);
        return (int) (result & ((1 << numberOfBits) - 1));
    }


    private static int[] split(long value, int divider, int length) {

        int[] result = new int[length];
        for (int i = 0; i < length - 1; i++) {
            result[i] = (int) (value % divider);
            value /= divider;
        }
        result[length - 1] = (int) value;
        return result;
    }


    private Long castTimestamp(Long timestamp, int fsp) {

        if (microsecondsPrecision && timestamp != null && !timestamp.equals(invalidDateAndTimeRepresentation)) {
            return timestamp * 1000 + fsp % 1000;
        }
        return timestamp;
    }
}
