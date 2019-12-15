package com.github.fevernova.framework.service.scheduler;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.Properties;


@Slf4j
public class Schedulerd implements AutoCloseable {


    private GlobalContext globalContext;

    private TaskContext taskContext;

    public static final String SCHEDULER_BARRIER = "scheduler.barrier";

    public static final String SCHEDULER_MONITOR = "scheduler.monitor";

    private Scheduler scheduler;


    public Schedulerd(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        try {
            Properties properties = new Properties();
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("quartz.properties");
            properties.load(in);
            SchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            this.scheduler = schedulerFactory.getScheduler();
        } catch (Exception e) {
            log.error("Init Schedulerd Error : ", e);
            Validate.isTrue(false);
        }
    }


    public void createJob(Class<? extends Job> clazz, String jobName, int intervalSec, Map<String, Object> deps) throws SchedulerException {

        JobKey jobKey = new JobKey(jobName);
        JobDetail jobDetail = JobBuilder.newJob(clazz).withIdentity(jobKey).build();
        jobDetail.getJobDataMap().putAll(deps);
        long firstTime = (Util.nowMS() / (intervalSec * 1000) + 1) * (intervalSec * 1000);
        Trigger trigger = TriggerBuilder.newTrigger().startAt(new Date(firstTime))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(intervalSec).repeatForever()).build();

        this.scheduler.scheduleJob(jobDetail, trigger);
        log.info(jobKey.getName() + " job has been added to scheduler");
    }


    public void pauseJob(String jobName) throws SchedulerException {

        JobKey jobKey = new JobKey(jobName);
        if (this.scheduler.getJobDetail(jobKey) != null) {
            this.scheduler.pauseJob(jobKey);
            log.info(jobKey.getName() + " job has been paused");
        }
    }


    public void resumeJob(String jobName) throws SchedulerException {

        JobKey jobKey = new JobKey(jobName);
        if (this.scheduler.getJobDetail(jobKey) != null) {
            this.scheduler.resumeJob(jobKey);
            log.info(jobKey.getName() + " job has been resumed");
        }
    }


    public void deleteJob(String jobName) throws SchedulerException {

        JobKey jobKey = new JobKey(jobName);
        if (this.scheduler.getJobDetail(jobKey) != null) {
            this.scheduler.deleteJob(jobKey);
            log.info(jobKey.getName() + " job has been deleted from scheduler");
        }
    }


    public void start() throws SchedulerException {

        this.scheduler.start();
        log.info("scheduler job has been started");
    }


    public void close() throws Exception {

        this.scheduler.shutdown();
        log.info("scheduler job has been closed");
    }

}
