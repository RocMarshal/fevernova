package com.github.fevernova.task.performance;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.selector.IntSelector;
import com.github.fevernova.framework.metric.evaluate.NoMetricEvaluate;
import com.github.fevernova.framework.service.config.TaskConfig;
import com.github.fevernova.framework.task.BaseTask;
import com.github.fevernova.framework.task.Manager;
import com.github.fevernova.framework.task.TaskTopology;
import com.github.fevernova.kafka.data.KafkaDataFactory;

import java.util.concurrent.atomic.AtomicInteger;


public class Task extends BaseTask {


    int sourceParallelism = 0;

    int parserParallelism = 0;

    int sinkParallelism = 0;


    public Task(TaskContext context, JobTags tags) {

        super(context, tags);
        context.put(Constants.INPUTCHANNEL_ + Constants.SIZE, "1024");
        context.put(Constants.OUTPUTCHANNEL_ + Constants.SIZE, "512");
        this.sourceParallelism = context.getInteger(Constants.SOURCE_ + Constants.PARALLELISM, super.globalContext.getJobTags().getUnit());
        this.parserParallelism = context.getInteger(Constants.PARSER_ + Constants.PARALLELISM, super.globalContext.getJobTags().getUnit());
        this.sinkParallelism = context.getInteger(Constants.SINK_ + Constants.PARALLELISM, super.globalContext.getJobTags().getUnit());
    }


    @Override public BaseTask init() throws Exception {

        super.init();
        super.manager = Manager.getInstance(super.globalContext, super.context);
        TaskConfig taskConfig = TaskConfig.builder()
                .sourceClass(JobSource.class)
                .parserClass(JobParser.class)
                .sinkClass(JobSink.class)
                .inputDataFactoryClass(KafkaDataFactory.class)
                .outputDataFactoryClass(KafkaDataFactory.class)
                .inputSelectorClass(IntSelector.class)
                .outputSelectorClass(IntSelector.class)
                .sourceParallelism(this.sourceParallelism)
                .parserParallelism(this.parserParallelism)
                .sinkParallelism(this.sinkParallelism)
                .sourceAvailbleNum(new AtomicInteger(this.sourceParallelism))
                .parserAvailbleNum(new AtomicInteger(this.parserParallelism))
                .sinkAvailbleNum(new AtomicInteger(this.sinkParallelism))
                .inputDynamicBalance(false)
                .outputDynamicBalance(false)
                .metricEvaluateClass(NoMetricEvaluate.class)
                .build();
        TaskTopology taskTopology = new TaskTopology(super.globalContext, super.context, taskConfig);
        super.manager.register(taskTopology);
        return this;
    }
}
