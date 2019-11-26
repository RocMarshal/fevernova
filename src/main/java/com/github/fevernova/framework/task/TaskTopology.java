package com.github.fevernova.framework.task;


import com.github.fevernova.framework.autoscale.Agent;
import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEventFactory;
import com.github.fevernova.framework.component.Component;
import com.github.fevernova.framework.component.ComponentChangeMode;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.channel.RingbufferChannel;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.framework.component.sink.AbstractSink;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.metric.evaluate.MetricEvaluate;
import com.github.fevernova.framework.service.config.TaskConfig;
import com.google.common.collect.Lists;
import com.lmax.disruptor.EventProcessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Slf4j public class TaskTopology implements AutoCloseable {


    protected final GlobalContext globalContext;

    protected final TaskConfig taskConfig;

    protected final TaskContext sourceContext;

    protected final TaskContext parserContext;

    protected final TaskContext sinkContext;

    protected final TaskContext inputChannelContext;

    protected final TaskContext outputChannelContext;

    protected final TaskContext inputChannelProxyContext = new TaskContext(Constants.INPUTCHANNELPROXY);

    protected final TaskContext outputChannelProxyContext = new TaskContext(Constants.OUTPUTCHANNELPROXY);

    protected final List<AbstractSource> sources = Lists.newArrayList();

    protected final List<EventProcessor> parsers = Lists.newArrayList();

    protected final List<EventProcessor> sinks = Lists.newArrayList();

    @Getter
    protected final List<Component> components = Lists.newArrayList();

    protected final List<RingbufferChannel> inputChannels = Lists.newArrayList();

    protected final List<RingbufferChannel> outputChannels = Lists.newArrayList();

    protected final List<ChannelProxy> inputProxys = Lists.newArrayList();

    protected final List<ChannelProxy> outputProxys = Lists.newArrayList();

    protected final DataEventFactory inputEventFactory;

    protected final DataEventFactory outputEventFactory;

    protected final ThreadPoolExecutor executors;

    @Getter
    protected List<Agent> agents;

    @Getter
    protected MetricEvaluate metricEvaluate;


    public TaskTopology(GlobalContext globalContext, TaskContext taskContext, TaskConfig taskConfig) throws Exception {

        this.executors = new ThreadPoolExecutor(16, 16, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        this.globalContext = globalContext;
        this.taskConfig = taskConfig;

        this.sourceContext = new TaskContext(Constants.SOURCE, taskContext.getSubProperties(Constants.SOURCE_));
        this.parserContext = new TaskContext(Constants.PARSER, taskContext.getSubProperties(Constants.PARSER_));
        this.sinkContext = new TaskContext(Constants.SINK, taskContext.getSubProperties(Constants.SINK_));
        this.inputChannelContext = new TaskContext(Constants.INPUTCHANNEL, taskContext.getSubProperties(Constants.INPUTCHANNEL_));
        this.outputChannelContext = new TaskContext(Constants.OUTPUTCHANNEL, taskContext.getSubProperties(Constants.OUTPUTCHANNEL_));

        this.sourceContext.put(Constants.PARALLELISM, String.valueOf(this.taskConfig.getSourceParallelism()));
        this.parserContext.put(Constants.PARALLELISM, String.valueOf(this.taskConfig.getParserParallelism()));
        this.sinkContext.put(Constants.PARALLELISM, String.valueOf(this.taskConfig.getSinkParallelism()));
        this.inputChannelProxyContext.put(Constants.PARALLELISM, this.sourceContext.get(Constants.PARALLELISM));
        this.outputChannelProxyContext.put(Constants.PARALLELISM, this.parserContext.get(Constants.PARALLELISM));

        this.inputEventFactory = new DataEventFactory(taskConfig.getInputDataFactoryClass().getConstructor().newInstance());
        this.outputEventFactory = new DataEventFactory(taskConfig.getOutputDataFactoryClass().getConstructor().newInstance());

        generateTaskTopology();
        generateAgent();
    }


    private void generateAgent() {

        Agent sourceAgent = new Agent(ComponentType.SOURCE, this.taskConfig.getSourceParallelism(), 1, this.taskConfig.getSourceAvailbleNum());
        Agent parserAgent = new Agent(ComponentType.PARSER, this.taskConfig.getParserParallelism(), 1, this.taskConfig.getParserAvailbleNum());
        Agent sinkAgent = new Agent(ComponentType.SINK, this.taskConfig.getSinkParallelism(), 1, this.taskConfig.getSinkAvailbleNum());
        this.agents = Lists.newArrayList(sinkAgent, parserAgent, sourceAgent);

        try {
            this.metricEvaluate = this.taskConfig.getMetricEvaluateClass().newInstance();
        } catch (Exception e) {
            log.error("TaskTopology metricEvaluate Error", e);
            Validate.isTrue(false);
        }
    }


    private void generateTaskTopology() {

        for (int i = 0; i < this.taskConfig.getSinkParallelism(); i++) {
            addSink(i);
        }
        for (int i = 0; i < this.taskConfig.getParserParallelism(); i++) {
            addParser(i);
        }
        for (int i = 0; i < this.taskConfig.getSourceParallelism(); i++) {
            addSource(i);
        }

    }


    private void addSource(int index) {

        try {
            ChannelProxy channelProxy = new ChannelProxy(this.globalContext, this.inputChannelProxyContext, this.inputChannels,
                                                         this.taskConfig.getParserAvailbleNum(), this.inputEventFactory,
                                                         this.taskConfig.getInputSelectorClass().getConstructor().newInstance(), index,
                                                         this.taskConfig.isInputDynamicBalance());
            AbstractSource source =
                    this.taskConfig.getSourceClass().getConstructor(GlobalContext.class, TaskContext.class, int.class, int.class, ChannelProxy.class).
                            newInstance(this.globalContext, this.sourceContext, index, 0, channelProxy);
            this.sources.add(source);
            this.components.add(source);
            this.inputProxys.add(channelProxy);
        } catch (Exception e) {
            log.error("TaskTopology AddSource Error", e);
            Validate.isTrue(false);
        }
    }


    private void addParser(int index) {

        RingbufferChannel channel =
                new RingbufferChannel(this.taskConfig.getSourceParallelism(), this.inputChannelContext, this.globalContext, this.inputEventFactory);
        this.inputChannels.add(channel);
        try {
            ChannelProxy channelProxy = new ChannelProxy(this.globalContext, this.outputChannelProxyContext, this.outputChannels,
                                                         this.taskConfig.getSinkAvailbleNum(), this.outputEventFactory,
                                                         this.taskConfig.getOutputSelectorClass().getConstructor().newInstance(), index,
                                                         this.taskConfig.isOutputDynamicBalance());
            AbstractParser parser =
                    this.taskConfig.getParserClass().getConstructor(GlobalContext.class, TaskContext.class, int.class, int.class, ChannelProxy.class)
                            .newInstance(this.globalContext, this.parserContext, index, this.taskConfig.getSourceParallelism(), channelProxy);
            this.parsers.add(channel.integrate(parser));
            this.components.add(parser);
            this.outputProxys.add(channelProxy);
        } catch (Exception e) {
            log.error("TaskTopology AddParser Error", e);
            Validate.isTrue(false);
        }
    }


    private void addSink(int index) {

        RingbufferChannel channel =
                new RingbufferChannel(this.taskConfig.getParserParallelism(), this.outputChannelContext, this.globalContext, this.outputEventFactory);
        this.outputChannels.add(channel);
        try {
            AbstractSink sink = this.taskConfig.getSinkClass().getConstructor(GlobalContext.class, TaskContext.class, int.class, int.class)
                    .newInstance(this.globalContext, this.sinkContext, index, this.taskConfig.getParserParallelism());
            this.sinks.add(channel.integrate(sink));
            this.components.add(sink);
        } catch (Exception e) {
            log.error("TaskTopology AddSink Error", e);
            Validate.isTrue(false);
        }
    }


    public void execute() {

        initChannelProxyAndComponent();
        this.sinks.forEach(sink -> this.executors.submit(sink));
        this.parsers.forEach(parser -> this.executors.submit(parser));
        this.sources.forEach(source -> this.executors.submit(source));
    }


    private void initChannelProxyAndComponent() {

        this.inputProxys.forEach(channelProxy -> channelProxy.init());
        this.outputProxys.forEach(channelProxy -> channelProxy.init());
        this.components.forEach(component -> component.init());
    }


    @Override
    public void close() {

        this.sources.forEach(source -> source.onShutdown());
        this.parsers.forEach(parser -> parser.halt());
        this.sinks.forEach(sink -> sink.halt());
        this.executors.shutdown();
    }


    public void sourcePause() {

        log.info("TaskTopology Source Pause");
        this.sources.forEach(source -> source.onPause());
    }


    public void sourceResume() {

        log.info("TaskTopology Source Resume");
        this.sources.forEach(source -> source.onResume());
    }


    private void changeParserAvailableNum(ComponentChangeMode change) {

        if (change == ComponentChangeMode.INCREMENT) {
            this.taskConfig.getParserAvailbleNum().incrementAndGet();
        } else {
            this.taskConfig.getParserAvailbleNum().decrementAndGet();
        }
    }


    private void changeSinkAvailableNum(ComponentChangeMode change) {

        if (change == ComponentChangeMode.INCREMENT) {
            this.taskConfig.getSinkAvailbleNum().incrementAndGet();
        } else {
            this.taskConfig.getSinkAvailbleNum().decrementAndGet();
        }
    }


    public void change(ComponentType component, ComponentChangeMode change) {

        if (component == ComponentType.PARSER) {
            changeParserAvailableNum(change);
        } else if (component == ComponentType.SINK) {
            changeSinkAvailableNum(change);
        }
    }


    public boolean needChange(ComponentType component, ComponentChangeMode change) {

        Validate.isTrue(ComponentType.SOURCE != component);
        int maxParallelism = (ComponentType.PARSER == component ? this.taskConfig.getParserParallelism() : this.taskConfig.getSinkParallelism());
        int currentParallelism =
                (ComponentType.PARSER == component ? this.taskConfig.getParserAvailbleNum().get() : this.taskConfig.getSinkAvailbleNum().get());

        if (ComponentChangeMode.INCREMENT == change) {
            return maxParallelism > currentParallelism;
        } else if (ComponentChangeMode.DECREMENT == change) {
            return currentParallelism > 1;
        } else {
            return false;
        }
    }

}
