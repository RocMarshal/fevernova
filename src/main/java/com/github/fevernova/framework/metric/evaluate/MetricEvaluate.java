package com.github.fevernova.framework.metric.evaluate;


import com.github.fevernova.framework.autoscale.Tendency;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.metric.MetricData;

import java.util.List;
import java.util.Map;


public interface MetricEvaluate {


    Map<ComponentType, Tendency> evaluate(Map<ComponentType, Map<String, List<MetricData>>> metrics);

    int getTimeLineNumber();//获取最近N次的TimeLine用于伸缩容的判断

    int getHighScore();//N次INCREMENT后，执行扩容

    int getLowScore();//N次DECREMENT

}
