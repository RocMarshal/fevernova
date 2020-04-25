package com.github.fevernova.io.rdb.schema;


import com.github.fevernova.io.data.TypeRouter;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;


@Getter
@Builder
@ToString
public class Column {


    private String name;

    private int seq;

    private String type;

    private boolean primaryKey;

    private String charset;

    private boolean ignore;

    private TypeRouter typeRouter;

    private String escapeLetter;


    public String escapeName() {

        return this.escapeLetter + this.name + this.escapeLetter;
    }
}
