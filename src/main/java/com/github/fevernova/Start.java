package com.github.fevernova;


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.service.config.LoadTaskContext;
import com.github.fevernova.framework.task.BaseTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.lang3.Validate;
import org.slf4j.LoggerFactory;

import static com.github.fevernova.CMD.*;


@Slf4j
public class Start {


    public static void main(String[] args) {

        try {
            CommandLine commandLine = (new DefaultParser()).parse(OPTIONS, args);

            JobTags jobTags = JobTags.builder()
                    .jobType(commandLine.getOptionValue(JOBTYPE.getLongOpt()))
                    .jobId(commandLine.getOptionValue(JOBID.getLongOpt()))
                    .cluster(commandLine.getOptionValue(CLUSTER.getLongOpt()))
                    .unit(Integer.parseInt(commandLine.getOptionValue(UNIT.getLongOpt())))
                    .level(commandLine.getOptionValue(LEVEL.getLongOpt()))
                    .deployment(commandLine.getOptionValue(DEPLOYMENTNAME.getLongOpt()))
                    .podName(commandLine.getOptionValue(PODNAME.getLongOpt()))
                    .podTotalNum(Integer.parseInt(commandLine.getOptionValue(PODTOTALNUM.getLongOpt())))
                    .podIndex(Integer.parseInt(commandLine.getOptionValue(PODINDEX.getLongOpt())))
                    .build();

            initLogBack(commandLine.getOptionValue(LOGCONFIG.getLongOpt()));

            TaskContext context = null;
            if (commandLine.hasOption(CONFIGPATH.getLongOpt())) {
                String configPath = commandLine.getOptionValue(CONFIGPATH.getLongOpt());
                if (configPath.endsWith(".property")) {
                    context = LoadTaskContext.load(configPath);
                } else {
                    Validate.isTrue(false, "config suffix must be .property");
                }
            }
            String clazzName = "com.github.fevernova.task." + jobTags.getJobType() + ".Task";
            Class<? extends BaseTask> clazz = Util.findClass(clazzName);
            BaseTask task = clazz.getConstructor(TaskContext.class, JobTags.class).newInstance(context, jobTags);
            task.init().recovery().start();
        } catch (Throwable e) {
            log.error("Job Start Error :", e);
            e.printStackTrace();
            System.exit(0);
        }
    }


    public static void initLogBack(String fn) {

        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            loggerContext.reset();
            JoranConfigurator joranConfigurator = new JoranConfigurator();
            joranConfigurator.setContext(loggerContext);
            joranConfigurator.doConfigure(fn);
        } catch (JoranException e) {
            e.printStackTrace();
        }
    }
}
