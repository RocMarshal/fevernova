package com.github.fevernova.task.exchange;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.selector.BytesSelector;
import com.github.fevernova.framework.component.channel.selector.LongSelector;
import com.github.fevernova.framework.metric.evaluate.NoMetricEvaluate;
import com.github.fevernova.framework.service.config.TaskConfig;
import com.github.fevernova.framework.task.BaseTask;
import com.github.fevernova.framework.task.Manager;
import com.github.fevernova.framework.task.TaskTopology;
import com.github.fevernova.io.kafka.data.KafkaDataFactory;
import com.github.fevernova.task.exchange.data.result.OrderMatchFactory;

import java.util.concurrent.atomic.AtomicInteger;


public class Task extends BaseTask {


    int parserInitParallelism = 0;

    int sinkInitParallelism = 0;


    public Task(TaskContext context, JobTags tags) {

        super(context, tags);
        this.parserInitParallelism = context.getInteger(Constants.PARSER_ + Constants.PARALLELISM, this.globalContext.getJobTags().getUnit());
        this.sinkInitParallelism = context.getInteger(Constants.SINK_ + Constants.PARALLELISM, this.globalContext.getJobTags().getUnit());
    }


    @Override public BaseTask init() throws Exception {

        super.init();
        TaskConfig taskConfig = TaskConfig.builder()
                .sourceClass(JobSource.class)
                .parserClass(JobParser.class)
                .sinkClass(JobSink.class)
                .inputDataFactoryClass(KafkaDataFactory.class)
                .outputDataFactoryClass(OrderMatchFactory.class)
                .inputSelectorClass(BytesSelector.class)
                .outputSelectorClass(LongSelector.class)
                .sourceParallelism(1)
                .parserParallelism(this.parserInitParallelism)
                .sinkParallelism(this.sinkInitParallelism)
                .sourceAvailbleNum(new AtomicInteger(1))
                .parserAvailbleNum(new AtomicInteger(this.parserInitParallelism))
                .sinkAvailbleNum(new AtomicInteger(this.sinkInitParallelism))
                .inputDynamicBalance(false)
                .outputDynamicBalance(false)
                .metricEvaluateClass(NoMetricEvaluate.class)
                .build();
        TaskTopology taskTopology = new TaskTopology(super.globalContext, super.context, taskConfig);
        super.manager.register(taskTopology);
        return this;
    }
}
