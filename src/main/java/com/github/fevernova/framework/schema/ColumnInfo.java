package com.github.fevernova.framework.schema;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public class ColumnInfo {


    private Class<? extends UData> clazz;

    private UData uData;

    private String sourceColumnName;

    private MethodType fromType;

    private String targetColumnName;

    private String targetTypeEnum;

}
