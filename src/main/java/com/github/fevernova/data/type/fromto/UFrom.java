package com.github.fevernova.data.type.fromto;


import java.util.Date;


public interface UFrom {


    void from(final Boolean p);

    void from(final Integer p);

    void from(final Long p);

    void from(final Float p);

    void from(final Double p);

    void from(final String p);

    void from(final Date p);

    void from(final byte[] p);

}
