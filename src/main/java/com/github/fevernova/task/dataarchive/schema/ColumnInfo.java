package com.github.fevernova.task.dataarchive.schema;


import com.github.fevernova.data.type.UData;
import com.github.fevernova.hdfs.orc.OrcTypeEnum;
import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public class ColumnInfo {


    private Class<? extends UData> clazz;

    private String sourceColumnName;

    private String targetColumnName;

    private String targetTypeEnum;


    private UData uData;

    private OrcTypeEnum orcTypeEnum;

}
