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
        this.parserInitParallelism =
                context.getInteger(Constants.PARSER_ + Constants.PARALLELISM, Math.min(this.globalContext.getJobTags().getUnit(), 2));
        this.sinkInitParallelism = context.getInteger(Constants.SINK_ + Constants.PARALLELISM, this.globalContext.getJobTags().getUnit());
    }


    @Override public BaseTask init() throws Exception {

        super.init();
        super.manager = Manager.getInstance(this.globalContext, this.context);
        TaskConfig taskConfig = TaskConfig.builder()
                .sourceClass(JobSource.class)
                .parserClass(JobParser.class)
                .sinkClass(JobSink.class)
                .inputDataFactoryClass(BinlogDataFactory.class)
                .outputDataFactoryClass(MessageDataFactory.class)
                .inputSelectorClass(StringSelector.class)
                .outputSelectorClass(StringSelector.class)
                .sourceParallelism(1)
                .parserParallelism(Math.min(this.globalContext.getJobTags().getUnit(), 2))
                .sinkParallelism(this.globalContext.getJobTags().getUnit() + 1)
                .sourceAvailbleNum(new AtomicInteger(1))
                .parserAvailbleNum(new AtomicInteger(this.parserInitParallelism))
                .sinkAvailbleNum(new AtomicInteger(this.sinkInitParallelism))
                .inputDynamicBalance(false)
                .outputDynamicBalance(false)
                .metricEvaluateClass(NoMetricEvaluate.class)
                .build();
        TaskTopology taskTopology = new TaskTopology(globalContext, this.context, taskConfig);
        super.manager.register(taskTopology);
        return this;
    }
}