package com.github.fevernova.framework.common.context;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@Builder
@ToString
public class JobTags {


    private String jobType;

    private String jobId;

    private String cluster;

    private int unit;

    private String level;

    private String deployment;

    private String podName;

    private int podTotalNum;

    private int podIndex;

}
