package com.github.fevernova.framework.service.barrier;


import com.github.fevernova.framework.task.Manager;
import org.quartz.Job;
import org.quartz.JobExecutionContext;


public class BarrierGeneratorJob implements Job {


    @Override public void execute(JobExecutionContext jobExecutionContext) {

        Manager.getInstance().getBarrierService().generateBarrier();
    }
}
