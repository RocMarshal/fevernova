package com.github.fevernova.task.marketdetail;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractSink;
import com.github.fevernova.task.marketdetail.data.OrderDetail;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RTopic;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.Map;


@Slf4j
public class JobSink extends AbstractSink {


    private Redisson redis;

    private Map<Integer, RTopic> channels = Maps.newHashMap();

    private Integer currentSymbolId;

    private RTopic currentTopic;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        Config redisConfig = new Config();
        SingleServerConfig singleServerConfig = redisConfig.useSingleServer();
        singleServerConfig.setAddress(taskContext.get("address"));
        singleServerConfig.setPassword(taskContext.get("password"));
        singleServerConfig.setDatabase(taskContext.getInteger("dbnum", 0));
        singleServerConfig.setClientName(super.named.render(true));
        this.redis = (Redisson) Redisson.create(redisConfig);
    }


    @Override protected void handleEvent(Data event) {

        OrderDetail orderDetail = (OrderDetail) event;
        if (this.currentSymbolId != orderDetail.getSymbolId()) {
            this.currentSymbolId = orderDetail.getSymbolId();
            this.currentTopic = this.channels.get(this.currentSymbolId);
            if (this.currentTopic == null) {
                this.currentTopic = this.redis.getTopic("OrderDetail_" + orderDetail.getSymbolId());
                this.channels.put(orderDetail.getSymbolId(), this.currentTopic);
            }
        }
        this.currentTopic.publish(orderDetail.getBytes());
    }
}
