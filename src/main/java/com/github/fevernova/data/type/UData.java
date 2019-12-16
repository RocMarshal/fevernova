package com.github.fevernova.data.type;


import com.github.fevernova.data.type.fromto.UAbstFrom;
import com.github.fevernova.data.type.fromto.UAbstTo;
import com.github.fevernova.data.type.fromto.UFrom;
import com.github.fevernova.data.type.fromto.UTo;

import java.util.Date;


public abstract class UData<U> implements UFrom, UTo {


    private UAbstFrom<U> from;

    private UAbstTo<U> to;

    private boolean lazy;

    private LazyCall[] lazyCalls = new LazyCall[MethodType.values().length];

    private int lazyCallIndex = -1;


    public UData(boolean lazy) {

        this.lazy = lazy;
    }


    public void configure(UAbstFrom<U> from, UAbstTo<U> to) {

        this.from = from;
        this.to = to;
        this.to.setFrom(from);
    }


    public UData<U> from(final Object o, final MethodType type) {

        switch (type) {
            case BOOLEAN:
                from((Boolean) o);
                break;
            case INT:
                from((Integer) o);
                break;
            case LONG:
                from((Long) o);
                break;
            case FLOAT:
                from((Float) o);
                break;
            case DOUBLE:
                from((Double) o);
                break;
            case STRING:
                from((String) o);
                break;
            case BYTES:
                from((byte[]) o);
                break;
            case DATE:
                from((Date) o);
                break;
            default:
                this.from.unsupport();
        }
        return this;
    }


    @Override
    public void from(final Boolean p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.BOOLEAN.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<Boolean>(p, MethodType.BOOLEAN) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override
    public void from(final Integer p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.INT.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<Integer>(p, MethodType.INT) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override
    public void from(final Long p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.LONG.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<Long>(p, MethodType.LONG) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override
    public void from(final Float p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.FLOAT.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<Float>(p, MethodType.FLOAT) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override
    public void from(final Double p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.DOUBLE.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<Double>(p, MethodType.DOUBLE) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override
    public void from(final String p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.STRING.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<String>(p, MethodType.STRING) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override public void from(byte[] p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.BYTES.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<byte[]>(p, MethodType.BYTES) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override
    public void from(Date p) {

        if (this.lazy) {
            this.lazyCallIndex = MethodType.DATE.ordinal();
            if (this.lazyCalls[this.lazyCallIndex] == null) {
                this.lazyCalls[this.lazyCallIndex] = new LazyCall<Date>(p, MethodType.DATE) {


                    @Override void exec() {

                        if (raw == null) {
                            from.setData(null);
                            return;
                        }
                        from.from(raw);
                    }
                };
            } else {
                this.lazyCalls[this.lazyCallIndex].raw = p;
            }
            return;
        } else {
            if (p == null) {
                this.from.setData(null);
                return;
            }
            this.from.from(p);
        }
    }


    @Override
    public Boolean toBoolean() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.BOOLEAN == lazyCall.type) {
                return (Boolean) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toBoolean();
    }


    @Override
    public Integer toInt() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.INT == lazyCall.type) {
                return (Integer) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toInt();
    }


    @Override
    public Long toLong() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.LONG == lazyCall.type) {
                return (Long) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toLong();
    }


    @Override
    public Float toFloat() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.FLOAT == lazyCall.type) {
                return (Float) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toFloat();
    }


    @Override
    public Double toDouble() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.DOUBLE == lazyCall.type) {
                return (Double) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toDouble();
    }


    @Override
    public String toStr() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.STRING == lazyCall.type) {
                return (String) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toStr();
    }


    @Override public byte[] toBytes() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.BYTES == lazyCall.type) {
                return (byte[]) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toBytes();
    }


    @Override
    public Date toDate() {

        if (this.lazy) {
            LazyCall lazyCall = this.lazyCalls[this.lazyCallIndex];
            if (MethodType.DATE == lazyCall.type) {
                return (Date) lazyCall.raw;
            }
            lazyCall.exec();
        }

        if (this.from.getData() == null) {
            return null;
        }
        return this.to.toDate();
    }


    public Object to(final MethodType type) {

        switch (type) {
            case BOOLEAN:
                return toBoolean();
            case INT:
                return toInt();
            case LONG:
                return toLong();
            case FLOAT:
                return toFloat();
            case DOUBLE:
                return toDouble();
            case STRING:
                return toStr();
            case BYTES:
                return toBytes();
            case DATE:
                return toDate();
            default:
                this.to.unsupport();
                return null;
        }
    }


    abstract class LazyCall<R> {


        R raw;

        MethodType type;


        LazyCall(R raw, MethodType type) {

            this.raw = raw;
            this.type = type;
        }


        abstract void exec();
    }
}
