package com.github.fevernova.task.rdb;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.selector.IntSelector;
import com.github.fevernova.framework.metric.evaluate.NoMetricEvaluate;
import com.github.fevernova.framework.service.config.TaskConfig;
import com.github.fevernova.framework.task.BaseTask;
import com.github.fevernova.framework.task.TaskTopology;
import com.github.fevernova.task.rdb.data.ListDataFactory;

import java.util.concurrent.atomic.AtomicInteger;


public class Task extends BaseTask {


    int sourceParallelism = 1;

    int parserParallelism = 0;

    int sinkParallelism = 0;


    public Task(TaskContext context, JobTags tags) {

        super(context, tags);
        context.put(Constants.INPUTCHANNEL_ + Constants.SIZE, "1024");
        context.put(Constants.OUTPUTCHANNEL_ + Constants.SIZE, "1024");
        this.parserParallelism = context.getInteger(Constants.PARSER_ + Constants.PARALLELISM, 1);
        this.sinkParallelism = context.getInteger(Constants.SINK_ + Constants.PARALLELISM, 1);
    }


    @Override public BaseTask init() throws Exception {

        super.init();
        super.manager.register(new TaskTopology(super.globalContext, super.context, TaskConfig.builder()
                .sourceClass(JobSource.class)
                .parserClass(JobParser.class)
                .sinkClass(JobSink.class)
                .inputDataFactoryClass(ListDataFactory.class)
                .outputDataFactoryClass(ListDataFactory.class)
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
                .build()));
        return this;
    }
}
