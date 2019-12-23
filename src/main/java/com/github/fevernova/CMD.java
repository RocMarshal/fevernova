package com.github.fevernova;


import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class CMD {


    static final Option JOBTYPE = Option.builder().longOpt("jobtype").hasArg(true).required(true).desc("Job类型").build();

    static final Option JOBID = Option.builder().longOpt("jobid").hasArg(true).required(true).desc("JobId").build();

    static final Option CLUSTER = Option.builder().longOpt("cluster").hasArg(true).required(true).desc("集群").build();

    static final Option UNIT = Option.builder().longOpt("unit").hasArg(true).required(true).desc("资源规格").build();

    static final Option LEVEL = Option.builder().longOpt("level").hasArg(true).required(true).desc("任务级别").build();

    static final Option LOGCONFIG = Option.builder().longOpt("logconfig").hasArg(true).required(true).desc("日志配置文件的路径").build();

    static final Option CONFIGURL = Option.builder().longOpt("configurl").hasArg(true).required(false).desc("配置的URL").build();

    static final Option CONFIGPATH = Option.builder().longOpt("configpath").hasArg(true).required(false).desc("配置文件路径").build();

    static final Option DEPLOYMENTNAME = Option.builder().longOpt("deploymentname").hasArg(true).required(true).desc("deployment名字").build();

    static final Option PODNAME = Option.builder().longOpt("podname").hasArg(true).required(true).desc("pod名字").build();

    static final Option PODTOTALNUM = Option.builder().longOpt("podtotalnum").hasArg(true).required(true).desc("pod名字").build();

    static final Option PODINDEX = Option.builder().longOpt("podindex").hasArg(true).required(true).desc("pod名字").build();

    static final Options OPTIONS = new Options();

    static {
        OPTIONS.addOption(JOBTYPE).addOption(JOBID).addOption(CLUSTER).addOption(UNIT).addOption(LEVEL).addOption(LOGCONFIG).addOption(CONFIGPATH)
                .addOption(CONFIGURL).addOption(DEPLOYMENTNAME).addOption(PODNAME).addOption(PODTOTALNUM).addOption(PODINDEX);
    }

}
