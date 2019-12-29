package com.github.fevernova.framework.service.monitor;


import com.codahale.metrics.*;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.metric.MetricData;
import com.github.fevernova.framework.metric.UnitCounter;
import com.github.fevernova.framework.task.Manager;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;


@Slf4j(topic = "fevernova-monitor")
public class MetricReporter extends ScheduledReporter {


    protected MetricReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
                             TimeUnit durationUnit) {

        super(registry, name, filter, rateUnit, durationUnit);
    }


    public MetricReporter(MetricRegistry registry, String name) {

        this(registry, name, MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    }


    @Override public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms,
                                 SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

        Map<ComponentType, Map<String, List<MetricData>>> mapResult = Maps.newHashMap();
        counters.entrySet().forEach(entry -> {
            UnitCounter unitCounter = (UnitCounter) entry.getValue();
            MetricData metricData = MetricData.builder()
                    .metricKey(entry.getKey())
                    .metricName(unitCounter.getName())
                    .componentType(unitCounter.getComponentType())
                    .unit(unitCounter.getUnit())
                    .value(unitCounter.getCountAndReset())
                    .build();

            Map<String, List<MetricData>> metricByName = mapResult.get(metricData.getComponentType());
            if (metricByName == null) {
                metricByName = Maps.newHashMap();
                mapResult.put(metricData.getComponentType(), metricByName);
            }

            List<MetricData> metrics = metricByName.get(metricData.getMetricName());
            if (metrics == null) {
                metrics = Lists.newArrayList();
                metricByName.put(metricData.getMetricName(), metrics);
            }

            metrics.add(metricData);
            if (log.isInfoEnabled()) {
                log.info(metricData.toString());
            }
        });

        Manager.getInstance().getMonitorService().handleMetrics(mapResult);
    }
}
