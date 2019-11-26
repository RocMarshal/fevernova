package com.github.fevernova.framework.metric;


import com.github.fevernova.framework.component.ComponentType;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;


@Getter
@Builder
@ToString
public class MetricData {


    private String metricKey;

    private ComponentType componentType;

    private String metricName;

    private long value;

    private String unit;

}
