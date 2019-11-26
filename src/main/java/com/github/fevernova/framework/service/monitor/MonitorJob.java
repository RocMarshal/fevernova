package com.github.fevernova.framework.service.monitor;


import com.github.fevernova.framework.task.Manager;
import org.quartz.Job;
import org.quartz.JobExecutionContext;


public class MonitorJob implements Job {


    @Override public void execute(JobExecutionContext context) {

        Manager.getInstance().getMonitorService().report();
    }
}
