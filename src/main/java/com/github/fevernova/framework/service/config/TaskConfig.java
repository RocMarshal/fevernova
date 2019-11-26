package com.github.fevernova.framework.service.config;


import com.github.fevernova.framework.common.data.DataFactory;
import com.github.fevernova.framework.component.channel.selector.ISelector;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.framework.component.sink.AbstractSink;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.metric.evaluate.MetricEvaluate;
import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;


@Getter @Builder public class TaskConfig {


    private Class<? extends AbstractSource> sourceClass;

    private Class<? extends DataFactory> inputDataFactoryClass;

    private Class<? extends ISelector> inputSelectorClass;

    private Class<? extends AbstractParser> parserClass;

    private Class<? extends DataFactory> outputDataFactoryClass;

    private Class<? extends ISelector> outputSelectorClass;

    private Class<? extends AbstractSink> sinkClass;

    private int sourceParallelism;

    private int parserParallelism;

    private int sinkParallelism;

    private AtomicInteger sourceAvailbleNum;

    private AtomicInteger parserAvailbleNum;

    private AtomicInteger sinkAvailbleNum;

    private boolean inputDynamicBalance;

    private boolean outputDynamicBalance;

    private Class<? extends MetricEvaluate> metricEvaluateClass;

}
