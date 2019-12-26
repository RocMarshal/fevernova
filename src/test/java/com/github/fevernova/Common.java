package com.github.fevernova;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;


public class Common {


    public static GlobalContext createGlobalContext() {

        JobTags jobTags = JobTags.builder()
                .jobType("testtype")
                .jobId("testid")
                .cluster("testclr")
                .unit(1)
                .level("l3")
                .deployment("testdpl")
                .podName("testpod")
                .podTotalNum(3)
                .podIndex(0)
                .build();
        return GlobalContext.builder().jobTags(jobTags).build();
    }


    public static TaskContext createTaskContext() {

        return new TaskContext("test");
    }


    public static long warn() {

        long base = 1000000000L;

        long k = 0, j = 0;
        while (k++ < base) {
            j += k;
        }
        return j;
    }

}
