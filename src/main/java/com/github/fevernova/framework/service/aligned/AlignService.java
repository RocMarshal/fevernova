package com.github.fevernova.framework.service.aligned;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.google.common.collect.Maps;

import java.util.Map;


public class AlignService {


    private final GlobalContext globalContext;

    private final TaskContext taskContext;

    private final Map<String, Aligner> map = Maps.newHashMap();


    public AlignService(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
    }


    public Aligner getAligner(String name, int parties) {

        Aligner aligner = this.map.get(name);
        if (aligner == null) {
            aligner = new Aligner(parties);
            this.map.put(name, aligner);
        }
        return aligner;
    }

}
