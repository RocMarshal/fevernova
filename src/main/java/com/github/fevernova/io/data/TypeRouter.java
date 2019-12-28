package com.github.fevernova.io.data;


import com.github.fevernova.io.data.message.DataType;
import com.github.fevernova.io.data.type.MethodType;
import com.github.fevernova.io.data.type.UData;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;


@Getter
@Builder
@ToString
public class TypeRouter {


    private UData uData;

    private MethodType from;

    private MethodType to;

    private DataType targetType;

}
