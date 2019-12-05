package com.github.fevernova.task.logdist;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.task.BaseTask;


public class Task extends BaseTask {


    int parserInitParallelism = 0;

    int sinkInitParallelism = 0;


    public Task(TaskContext context, JobTags tags) {

        super(context, tags);
        context.put(Constants.INPUTCHANNEL_ + Constants.SIZE, "1024");
        context.put(Constants.OUTPUTCHANNEL_ + Constants.SIZE, "512");
        this.parserInitParallelism =
                context.getInteger(Constants.PARSER_ + Constants.PARALLELISM, Math.min(this.globalContext.getJobTags().getUnit(), 3));
        this.sinkInitParallelism = context.getInteger(Constants.SINK_ + Constants.PARALLELISM, this.globalContext.getJobTags().getUnit());
    }


    @Override public BaseTask init() throws Exception {

        return null;
    }
}
