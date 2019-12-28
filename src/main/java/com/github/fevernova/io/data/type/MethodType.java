package com.github.fevernova.io.data.type;


import java.nio.ByteBuffer;
import java.util.Date;


public enum MethodType {

    BOOLEAN(1) {
        @Override public byte[] convertToBytes(Object s) {

            return ByteBuffer.allocate(bytesLength).put((byte) ((Boolean) s ? 1 : 0)).array();
        }


        @Override public Boolean parseFromBytes(byte[] s) {

            return ByteBuffer.wrap(s, 0, bytesLength).get() != 0 ? Boolean.TRUE : Boolean.FALSE;
        }


        @Override public Boolean parseFromString(String s) {

            return Boolean.valueOf(s);
        }
    }, INT(4) {
        @Override public byte[] convertToBytes(Object s) {

            return ByteBuffer.allocate(bytesLength).putInt((Integer) s).array();
        }


        @Override public Integer parseFromBytes(byte[] s) {

            return ByteBuffer.wrap(s, 0, bytesLength).getInt();
        }


        @Override public Integer parseFromString(String s) {

            return Integer.valueOf(s);
        }
    }, LONG(8) {
        @Override public byte[] convertToBytes(Object s) {

            return ByteBuffer.allocate(bytesLength).putLong((Long) s).array();
        }


        @Override public Long parseFromBytes(byte[] s) {

            return ByteBuffer.wrap(s, 0, bytesLength).getLong();
        }


        @Override public Long parseFromString(String s) {

            return Long.valueOf(s);
        }
    }, FLOAT(4) {
        @Override public byte[] convertToBytes(Object s) {

            return ByteBuffer.allocate(bytesLength).putFloat((Float) s).array();
        }


        @Override public Float parseFromBytes(byte[] s) {

            return ByteBuffer.wrap(s, 0, bytesLength).getFloat();
        }


        @Override public Float parseFromString(String s) {

            return Float.valueOf(s);
        }
    }, DOUBLE(8) {
        @Override public byte[] convertToBytes(Object s) {

            return ByteBuffer.allocate(bytesLength).putDouble((Double) s).array();
        }


        @Override public Double parseFromBytes(byte[] s) {

            return ByteBuffer.wrap(s, 0, bytesLength).getDouble();
        }


        @Override public Double parseFromString(String s) {

            return Double.valueOf(s);
        }
    }, STRING(Integer.MIN_VALUE) {
        @Override public byte[] convertToBytes(Object s) {

            return null;
        }


        @Override public String parseFromBytes(byte[] s) {

            return null;
        }


        @Override public String parseFromString(String s) {

            return null;
        }
    }, BYTES(Integer.MIN_VALUE) {
        @Override public byte[] convertToBytes(Object s) {

            return null;
        }


        @Override public byte[] parseFromBytes(byte[] s) {

            return null;
        }


        @Override public byte[] parseFromString(String s) {

            return null;
        }
    }, DATE(Integer.MIN_VALUE) {
        @Override public byte[] convertToBytes(Object s) {

            return null;
        }


        @Override public Date parseFromBytes(byte[] s) {

            return null;
        }


        @Override public Date parseFromString(String s) {

            return null;
        }
    };

    public final int bytesLength;


    MethodType(int bytesLength) {

        this.bytesLength = bytesLength;
    }


    public abstract <R> R parseFromBytes(byte[] s);

    public abstract <R> R parseFromString(String s);

    public abstract byte[] convertToBytes(Object s);

}
