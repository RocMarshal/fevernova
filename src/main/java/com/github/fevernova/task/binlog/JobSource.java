package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.structure.rb.IRingBuffer;
import com.github.fevernova.framework.common.structure.rb.SimpleRingBuffer;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.Optional;


@Slf4j
public class JobSource extends AbstractSource implements BinaryLogClient.EventListener, BinaryLogClient.LifecycleListener {


    private TaskContext dataSourceContext;

    private MysqlDataSource mysqlDataSource;

    private BinaryLogClient mysqlClient;

    private IRingBuffer<Event> iRingBuffer;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.dataSourceContext = new TaskContext("mysql", super.taskContext.getSubProperties("mysql."));
        this.mysqlDataSource = new MysqlDataSource(this.dataSourceContext);
        this.mysqlClient = new BinaryLogClient(this.mysqlDataSource.getHost(), this.mysqlDataSource.getPort(), this.mysqlDataSource.getUsername(),
                                               this.mysqlDataSource.getPassword());
        this.iRingBuffer = new SimpleRingBuffer<>(128);
    }


    @Override public void init() {

        super.init();
        try {
            this.mysqlDataSource.initJDBC();
        } catch (Exception e) {
            log.error("source init error : ", e);
        }
        this.mysqlClient.setServerId(this.mysqlDataSource.getSlaveId());
        this.mysqlClient.registerLifecycleListener(this);
        this.mysqlClient.registerEventListener(this);
    }


    @Override public void work() {

        Optional<Event> oe = this.iRingBuffer.get();
        if (oe == null) {
            Util.sleepMS(1);
            waitTime(1_000_000l);
            return;
        }
        Event event = oe.get();
        Validate.notNull(event);
    }


    @Override public void onEvent(Event event) {

        this.iRingBuffer.add(event, 1);
    }


    @Override public void onConnect(BinaryLogClient client) {

    }


    @Override public void onDisconnect(BinaryLogClient client) {

    }


    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception ex) {

        log.error("Mysql Communication Failure ", ex);
        Validate.isTrue(false);
    }


    @Override
    public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {

        log.error("Mysql Deserialization Failure ", ex);
        Validate.isTrue(false);
    }


    @Override public void onShutdown() {

        super.onShutdown();
        try {
            this.mysqlDataSource.close();
            if (this.mysqlClient != null) {
                this.mysqlClient.unregisterLifecycleListener(this);
                this.mysqlClient.unregisterEventListener(this);
                this.mysqlClient.disconnect();
            }
        } catch (Exception e) {
            log.error("Source shutdown error :", e);
        }
    }
}
