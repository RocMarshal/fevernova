package com.github.fevernova.framework.service.aligned;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Map;


public class AlignService {


    private final Map<String, Aligner> map = Maps.newHashMap();

    @Getter
    private final boolean exactlyOnce;


    public AlignService(GlobalContext globalContext, TaskContext taskContext) {

        this.exactlyOnce = taskContext.getBoolean("exactlyonce", false);
    }


    public Aligner getAligner(String name, int parties) {

        if (!map.containsKey(name)) {
            map.put(name, new Aligner(parties));
        }
        return map.get(name);
    }

}
