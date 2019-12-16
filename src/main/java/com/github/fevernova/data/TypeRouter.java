package com.github.fevernova.data;


import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
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
