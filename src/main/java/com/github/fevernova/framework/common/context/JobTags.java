package com.github.fevernova.framework.common.context;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
@Builder
public class JobTags {


    private String jobType;

    private String jobId;

    private String cluster;

    private String deployment;

    private String pod;

    private String level;

    private int unit;

}
