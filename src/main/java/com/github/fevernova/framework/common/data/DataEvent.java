package com.github.fevernova.framework.common.data;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Setter
@Getter
@ToString
public class DataEvent<T extends Data> {


    private DataType dataType = DataType.DATA;

    private T data;

}
