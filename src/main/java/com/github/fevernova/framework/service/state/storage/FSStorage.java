package com.github.fevernova.framework.service.state.storage;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.AchieveClean;
import com.github.fevernova.framework.service.state.StateValue;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.Validate;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;


public class FSStorage extends IStorage {


    private String basePath;


    public FSStorage(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
        String basePathStr = taskContext.getString("basepath", "/tmp/fevernova/stat");
        JobTags tags = globalContext.getJobTags();
        String taskPathStr = String.format("/{}/{}/", tags.getJobType() + tags.getJobId(), tags.getPodTotalNum() + "-" + tags.getPodIndex());
        this.basePath = basePathStr + taskPathStr;
        Util.mkDir(new File(this.basePath));
    }


    @Override public void save(BarrierData barrierData, List<StateValue> stateValueList) {


        Util.writeFile(this.basePath + barrier2FileName(barrierData), JSON.toJSONBytes(stateValueList));
    }


    @Override public void achieve(BarrierData barrierData, AchieveClean achieveClean) {

        switch (achieveClean) {
            case CURRENT:
                File file = new File(this.basePath + barrier2FileName(barrierData));
                if (file.exists()) {
                    Validate.isTrue(file.delete());
                }
                return;
            case ALL:
                File[] allFiles = new File(this.basePath).listFiles();
                if (allFiles != null && allFiles.length > 0) {
                    for (File filea : allFiles) {
                        Validate.isTrue(filea.delete());
                    }
                }
                return;
            case BEFORE:
                String current = barrier2FileName(barrierData);
                File[] files = new File(this.basePath).listFiles();
                if (files != null && files.length > 0) {
                    for (File fileb : files) {
                        if (current.compareTo(fileb.getName()) > 0) {
                            Validate.isTrue(fileb.delete());
                        }
                    }
                }
                return;
        }
    }


    @Override public List<StateValue> recovery() {

        File[] files = new File(this.basePath).listFiles();
        if (files != null && files.length > 0) {
            Arrays.sort(files, Comparator.comparing(File::getName));
            File file = files[files.length - 1];
            String data = Util.readFile(file);
            List<StateValue> result = JSON.parseArray(data, StateValue.class);
            return result;
        }
        return Lists.newArrayList();
    }


    private String barrier2FileName(BarrierData barrierData) {

        return barrierData.getTimestamp() + "_" + barrierData.getBarrierId();
    }
}
