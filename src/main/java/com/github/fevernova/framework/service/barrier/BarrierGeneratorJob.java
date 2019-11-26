package com.github.fevernova.framework.service.barrier;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.task.Manager;
import com.google.common.io.Files;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.io.File;
import java.io.IOException;


@Slf4j
public class BarrierGeneratorJob implements Job {


    @Override public void execute(JobExecutionContext jobExecutionContext) {

        Manager.getInstance().getBarrierService().generateBarrier();
        writeBeatFile();
    }


    private void writeBeatFile() {

        try {
            File file = new File("/tmp/fevernova");
            Files.write(String.valueOf(Util.nowSec()).getBytes(), file);
        } catch (IOException e) {
            log.error("write heart beat file error", e);
        }
    }
}
