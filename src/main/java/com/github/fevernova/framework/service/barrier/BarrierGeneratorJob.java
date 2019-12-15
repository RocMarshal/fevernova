package com.github.fevernova.framework.service.barrier;


import com.github.fevernova.framework.task.Manager;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;


@Slf4j
public class BarrierGeneratorJob implements Job {


    @Override public void execute(JobExecutionContext jobExecutionContext) {

        Manager.getInstance().getBarrierService().generateBarrier();
    }
}
