package com.github.fevernova.framework.service.state.storage;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.AchieveClean;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.framework.service.uniq.WireToOutputStream2;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.apache.commons.lang3.Validate;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;


@Slf4j
public class FSStorage extends IStorage {


    private static final String PRE_TXT = "txt_";

    private static final String PRE_BIN = "bin_";

    private String basePath;


    public FSStorage(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
        String basePathStr = taskContext.getString("basepath", "/tmp/fevernova/stat");
        JobTags tags = globalContext.getJobTags();
        String taskPathStr = "/" + tags.getJobType() + "-" + tags.getJobId() + "/" + tags.getPodTotalNum() + "-" + tags.getPodIndex() + "/";
        this.basePath = basePathStr + taskPathStr;
        Util.mkDir(new File(this.basePath));
    }


    @Override public void saveStateValue(BarrierData barrierData, List<StateValue> stateValueList) {

        String stateFilePath = this.basePath + barrier2FileName(barrierData, PRE_TXT);
        log.info("FSStorage saveStateValue : " + stateFilePath);
        Util.writeFile(stateFilePath, JSON.toJSONBytes(stateValueList));
    }


    public void saveBinary(BarrierData barrierData, WriteBytesMarshallable obj) {

        String stateFilePath = this.basePath + barrier2FileName(barrierData, PRE_BIN);
        log.info("FSStorage saveBinary : " + stateFilePath);
        final Path path = Paths.get(stateFilePath);
        log.info("Writing state to {} ...", path);
        try (final OutputStream os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
                final OutputStream bos = new BufferedOutputStream(os);
                final WireToOutputStream2 wireToOutputStream = new WireToOutputStream2(WireType.RAW, bos)) {

            final Wire wire = wireToOutputStream.getWire();
            wire.writeBytes(obj);
            log.info("done serializing, flushing {} ...", path);
            wireToOutputStream.flush();
            //bos.flush();
            log.info("completed {}", path);
        } catch (final IOException ex) {
            log.error("Can not write snapshot file: ", ex);
            Validate.isTrue(false);
        }
    }


    @Override public void achieveStateValue(BarrierData barrierData, AchieveClean achieveClean) {

        switch (achieveClean) {
            case CURRENT:
                File file = new File(this.basePath + barrier2FileName(barrierData, PRE_TXT));
                if (file.exists()) {
                    log.info("FSStorage delete : " + file.getName());
                    Validate.isTrue(file.delete());
                }
                return;
            case ALL:
                File[] allFiles = new File(this.basePath).listFiles(pathname -> pathname.getName().startsWith(PRE_TXT));
                if (allFiles != null && allFiles.length > 0) {
                    for (File filea : allFiles) {
                        log.info("FSStorage delete : " + filea.getName());
                        Validate.isTrue(filea.delete());
                    }
                }
                return;
            case BEFORE:
                String current = barrier2FileName(barrierData, PRE_TXT);
                File[] files = new File(this.basePath).listFiles(pathname -> pathname.getName().startsWith(PRE_TXT));
                if (files != null && files.length > 0) {
                    for (File fileb : files) {
                        if (current.compareTo(fileb.getName()) > 0) {
                            log.info("FSStorage delete : " + fileb.getName());
                            Validate.isTrue(fileb.delete());
                        }
                    }
                }
                return;
        }
    }


    @Override public List<StateValue> recoveryStateValue() {

        File[] files = new File(this.basePath).listFiles(pathname -> pathname.getName().startsWith(PRE_TXT));
        if (files != null && files.length > 0) {
            Arrays.sort(files, Comparator.comparing(File::getName));
            File file = files[files.length - 1];
            String data = Util.readFile(file);
            List<StateValue> result = JSON.parseArray(data, StateValue.class);
            return result;
        }
        return Lists.newArrayList();
    }


    private String barrier2FileName(BarrierData barrierData, String pre) {

        return pre + barrierData.getTimestamp() + "_" + barrierData.getBarrierId();
    }
}
