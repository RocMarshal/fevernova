package com.github.fevernova.framework.metric.evaluate;


import com.github.fevernova.framework.autoscale.Tendency;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.metric.MetricData;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;


public class NoMetricEvaluate implements MetricEvaluate {


    @Override public Map<ComponentType, Tendency> evaluate(Map<ComponentType, Map<String, List<MetricData>>> metrics) {

        return Maps.newHashMap();
    }


    @Override public int getTimeLineNumber() {

        return 5;
    }


    @Override public int getHighScore() {

        return 5;
    }


    @Override public int getLowScore() {

        return 5;
    }
}
