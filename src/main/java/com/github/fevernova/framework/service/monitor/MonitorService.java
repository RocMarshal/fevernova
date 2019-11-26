package com.github.fevernova.framework.service.monitor;


import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.github.fevernova.framework.autoscale.ActionTags;
import com.github.fevernova.framework.autoscale.Agent;
import com.github.fevernova.framework.autoscale.Tendency;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.metric.MetricData;
import com.github.fevernova.framework.metric.evaluate.MetricEvaluate;
import com.github.fevernova.framework.task.Manager;
import com.github.fevernova.framework.task.TaskTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;


@Slf4j
public class MonitorService {


    private GlobalContext globalContext;

    private TaskContext taskContext;

    private MetricRegistry metrics;

    private MetricReporter metricReporter;

    private List<Agent> agents;

    private MetricEvaluate metricEvaluate;


    public MonitorService(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.metrics = new MetricRegistry();
        this.metricReporter = new MetricReporter(this.metrics, "metric-reporter");
    }


    public void register(TaskTopology taskTopology) {

        this.agents = taskTopology.getAgents();
        this.metricEvaluate = taskTopology.getMetricEvaluate();
    }


    public <T extends Metric> T register(String name, T metric) {

        return this.metrics.register(name, metric);
    }


    public void report() {

        this.metricReporter.report();
    }


    public void handleMetrics(Map<ComponentType, Map<String, List<MetricData>>> metricsMap) {

        Map<ComponentType, Tendency> result = this.metricEvaluate.evaluate(metricsMap);
        for (Agent agent : this.agents) {
            if (result.containsKey(agent.getComponentType())) {
                agent.addTimeLine(result.get(agent.getComponentType()));
                Pair<ComponentType, ActionTags> pair = agent.triggerAction(
                        this.metricEvaluate.getTimeLineNumber(), this.metricEvaluate.getHighScore(), this.metricEvaluate.getLowScore());

                if (pair.getRight().change) {
                    Manager.getInstance().updateTaskTopology(pair.getLeft(), pair.getRight().componentChangeMode);
                    agent.toNextState();
                }
            }
        }
    }
}
