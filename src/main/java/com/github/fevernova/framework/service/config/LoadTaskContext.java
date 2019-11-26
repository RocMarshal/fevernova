package com.github.fevernova.framework.service.config;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.TaskContext;

import java.io.IOException;
import java.util.Properties;


public class LoadTaskContext {


    public static TaskContext load(String configPath) throws IOException {

        TaskContext context = new TaskContext(Constants.PROJECT_NAME, configPath);
        return commonDeal(context);
    }


    public static TaskContext load(Properties properties) throws IOException {

        TaskContext context = new TaskContext(Constants.PROJECT_NAME, properties);
        return commonDeal(context);
    }


    private static TaskContext commonDeal(TaskContext context) {

        String taskType = context.get(Constants.PROJECT_NAME);
        return new TaskContext(taskType, context.getSubProperties(taskType + "."));
    }

}
