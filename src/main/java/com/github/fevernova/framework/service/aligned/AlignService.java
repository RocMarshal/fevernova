package com.github.fevernova.framework.service.aligned;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.google.common.collect.Maps;

import java.util.Map;


public class AlignService {


    private final Map<String, Aligner> map = Maps.newHashMap();


    public AlignService(GlobalContext globalContext, TaskContext taskContext) {

    }


    public Aligner getAligner(String name, int parties) {

        if (!map.containsKey(name)) {
            map.put(name, new Aligner(parties));
        }
        return map.get(name);
    }

}
