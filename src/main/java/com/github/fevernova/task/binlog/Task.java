package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.selector.StringSelector;
import com.github.fevernova.framework.metric.evaluate.NoMetricEvaluate;
import com.github.fevernova.framework.service.config.TaskConfig;
import com.github.fevernova.framework.task.BaseTask;
import com.github.fevernova.framework.task.Manager;
import com.github.fevernova.framework.task.TaskTopology;
import com.github.fevernova.task.binlog.data.BinlogDataFactory;
import com.github.fevernova.task.binlog.data.MessageDataFactory;

import java.util.concurrent.atomic.AtomicInteger;


public class Task extends BaseTask {


    int parserInitParallelism = 0;

    int sinkInitParallelism = 0;


    public Task(TaskContext context, JobTags tags) {

        super(context, tags);
        context.put(Constants.INPUTCHANNEL_ + Constants.SIZE, "1024");
        context.put(Constants.OUTPUTCHANNEL_ + Constants.SIZE, "512");

        int unit = super.globalContext.getJobTags().getUnit();
        this.parserInitParallelism = context.getInteger(Constants.PARSER_ + Constants.PARALLELISM, Math.min(unit, 2));
        this.sinkInitParallelism = context.getInteger(Constants.SINK_ + Constants.PARALLELISM, unit);
    }


    @Override public BaseTask init() throws Exception {

        super.init();
        TaskConfig taskConfig = TaskConfig.builder()
                .sourceClass(JobSource.class)
                .parserClass(JobParser.class)
                .sinkClass(JobSink.class)
                .inputDataFactoryClass(BinlogDataFactory.class)
                .outputDataFactoryClass(MessageDataFactory.class)
                .inputSelectorClass(StringSelector.class)
                .outputSelectorClass(StringSelector.class)
                .sourceParallelism(1)
                .parserParallelism(this.parserInitParallelism)
                .sinkParallelism(this.sinkInitParallelism + 1)
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
