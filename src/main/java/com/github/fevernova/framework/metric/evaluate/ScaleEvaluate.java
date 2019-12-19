package com.github.fevernova.framework.metric.evaluate;


import com.github.fevernova.framework.autoscale.Tendency;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.metric.MetricData;
import com.github.fevernova.framework.metric.MetricName;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;


public class ScaleEvaluate implements MetricEvaluate {


    private static final long WAIT_TIME_TO_EXPAND = 15 * 1000;

    private static final long WAIT_TIME_TO_SHRINK = 45 * 1000;


    @Override public Map<ComponentType, Tendency> evaluate(Map<ComponentType, Map<String, List<MetricData>>> metrics) {

        Map<String, List<MetricData>> sinkMetrics = metrics.get(ComponentType.SINK);
        List<MetricData> waitTimeMetrics = sinkMetrics.get(MetricName.WAIT_TIME);
        long max = waitTimeMetrics.stream().mapToLong(value -> value.getValue()).max().getAsLong();
        long min = waitTimeMetrics.stream().mapToLong(value -> value.getValue()).min().getAsLong();

        Map<ComponentType, Tendency> result = Maps.newHashMap();
        if (max <= WAIT_TIME_TO_EXPAND) {
            result.put(ComponentType.SINK, Tendency.INCREMENT);
        } else if (min >= WAIT_TIME_TO_SHRINK) {
            result.put(ComponentType.SINK, Tendency.DECREMENT);
        }
        return result;
    }


    @Override public int getTimeLineNumber() {

        return 5;
    }


    @Override public int getHighScore() {

        return 4;
    }


    @Override public int getLowScore() {

        return 4;
    }
}
