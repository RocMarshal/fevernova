package com.github.fevernova;


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.service.config.LoadTaskContext;
import com.github.fevernova.framework.task.BaseTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.LoggerFactory;


@Slf4j
public class Start {


    final static Option JOBTYPE = Option.builder().longOpt("jobtype").hasArg(true).required(true).desc("Job类型").build();

    final static Option JOBID = Option.builder().longOpt("jobid").hasArg(true).required(true).desc("JobId").build();

    final static Option CLUSTER = Option.builder().longOpt("cluster").hasArg(true).required(true).desc("集群").build();

    final static Option UNIT = Option.builder().longOpt("unit").hasArg(true).required(true).desc("资源规格").build();

    final static Option LEVEL = Option.builder().longOpt("level").hasArg(true).required(true).desc("任务级别").build();

    final static Option LOGCONFIG = Option.builder().longOpt("logconfig").hasArg(true).required(true).desc("日志配置文件的路径").build();

    final static Option CONFIGURL = Option.builder().longOpt("configurl").hasArg(true).required(false).desc("配置的URL").build();

    final static Option CONFIGPATH = Option.builder().longOpt("configpath").hasArg(true).required(false).desc("配置文件路径").build();

    final static Option DEPLOYMENTNAME = Option.builder().longOpt("deploymentname").hasArg(true).required(true).desc("deployment名字").build();

    final static Option PODNAME = Option.builder().longOpt("podname").hasArg(true).required(true).desc("pod名字").build();

    final static Option PODTOTALNUM = Option.builder().longOpt("podtotalnum").hasArg(true).required(true).desc("pod名字").build();

    final static Option PODINDEX = Option.builder().longOpt("podindex").hasArg(true).required(true).desc("pod名字").build();

    final static Options OPTIONS = new Options();

    static {
        OPTIONS.addOption(JOBTYPE).addOption(JOBID).addOption(CLUSTER).addOption(UNIT).addOption(LEVEL).addOption(LOGCONFIG).addOption(CONFIGPATH)
                .addOption(CONFIGURL).addOption(DEPLOYMENTNAME).addOption(PODNAME).addOption(PODTOTALNUM).addOption(PODINDEX);
    }

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

            String logConfig = commandLine.getOptionValue(LOGCONFIG.getLongOpt());
            initLogBack(logConfig);

            TaskContext context = null;
            if (commandLine.hasOption(CONFIGPATH.getLongOpt())) {
                String configPath = commandLine.getOptionValue(CONFIGPATH.getLongOpt());
                if (configPath.endsWith(".property")) {
                    context = LoadTaskContext.load(configPath);
                }
            } else {
                String configUrl = commandLine.getOptionValue(CONFIGURL.getLongOpt());
                //TODO 下载property文件保存在本地在loadconfig
            }

            String clazzName = "com.github.fevernova.task." + jobTags.getJobType() + ".Task";
            Class<? extends BaseTask> clazz = (Class<? extends BaseTask>) Class.forName(clazzName);
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
