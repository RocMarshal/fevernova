package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.structure.rb.IRingBuffer;
import com.github.fevernova.framework.common.structure.rb.SimpleRingBuffer;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.framework.service.checkpoint.ICheckPointSaver;
import com.github.fevernova.task.binlog.data.BinlogData;
import com.github.fevernova.task.binlog.data.MysqlCheckPoint;
import com.github.fevernova.task.binlog.util.MysqlDataSource;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.DeserializationHelper;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class JobSource extends AbstractSource<String, BinlogData> implements BinaryLogClient.EventListener, BinaryLogClient.LifecycleListener {


    private final ICheckPointSaver<MysqlCheckPoint> checkpoints;

    private final TaskContext dataSourceContext;

    private final MysqlDataSource mysqlDataSource;

    private final BinaryLogClient mysqlClient;

    private final IRingBuffer<Pair<String, Event>> iRingBuffer = new SimpleRingBuffer<>(128);

    //cache
    private Event tableMapEvent = null;

    private Map<String, byte[]> cacheColumnTypes = Maps.newHashMap();

    private Map<Long, Event> cacheTableMapEvent4Transaction = Maps.newHashMap();

    private Map<Long, TableMapEventData> cacheTableMap4BinlogClient;

    //use for checkpoint
    private String binlogFileName;

    private long binlogPosition;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.checkpoints = new CheckPointSaver<>();
        this.dataSourceContext = new TaskContext("mysql", super.taskContext.getSubProperties("mysql."));
        this.mysqlDataSource = new MysqlDataSource(this.dataSourceContext);
        try {
            this.mysqlDataSource.initJDBC();
        } catch (Exception e) {
            log.error("source init error : ", e);
        }
        this.mysqlClient = new BinaryLogClient(this.mysqlDataSource.getHost(), this.mysqlDataSource.getPort(), this.mysqlDataSource.getUsername(),
                                               this.mysqlDataSource.getPassword());
        this.mysqlClient.setServerId(this.mysqlDataSource.getSlaveId());
        this.mysqlClient.registerLifecycleListener(this);
        this.mysqlClient.registerEventListener(this);
        Pair<EventDeserializer, Map<Long, TableMapEventData>> ps = DeserializationHelper.create();
        this.mysqlClient.setEventDeserializer(ps.getKey());
        this.cacheTableMap4BinlogClient = ps.getValue();
        super.globalContext.getCustomContext().put(MysqlDataSource.class.getSimpleName(), this.mysqlDataSource);
    }


    @Override public void init() {

        super.init();

        //TODO checkpoint

        new Thread(() -> {

            try {
                mysqlClient.connect();
            } catch (Exception e) {
                super.globalContext.fatalError("mysql client connect error : ", e);
            }
        }).start();

    }


    @Override public void work() {

        Optional<Pair<String, Event>> oe = this.iRingBuffer.get();
        if (oe == null) {
            Util.sleepMS(1);
            waitTime(1_000_000l);
            return;
        }
        String tmpFileName = oe.get().getLeft();
        Event event = oe.get().getRight();
        Validate.notNull(event);

        EventType eventType = event.getHeader().getEventType();
        long dataTableId;
        switch (eventType) {
            case PRE_GA_WRITE_ROWS:
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                dataTableId = ((WriteRowsEventData) event.getData()).getTableId();
                break;
            case PRE_GA_UPDATE_ROWS:
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                dataTableId = ((UpdateRowsEventData) event.getData()).getTableId();
                break;
            case PRE_GA_DELETE_ROWS:
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                dataTableId = ((DeleteRowsEventData) event.getData()).getTableId();
                break;
            case TABLE_MAP:
                dataTableId = ((TableMapEventData) event.getData()).getTableId();
                if (this.tableMapEvent == null) {
                    this.tableMapEvent = event;
                    this.binlogFileName = tmpFileName;
                    this.binlogPosition = ((EventHeaderV4) this.tableMapEvent.getHeader()).getPosition();
                } else if (dataTableId != ((TableMapEventData) this.tableMapEvent.getData()).getTableId()) {
                    this.cacheTableMapEvent4Transaction.put(dataTableId, event);
                }
                return;
            case XID:
                this.tableMapEvent = null;
                this.cacheTableMapEvent4Transaction.clear();
                if (this.cacheTableMap4BinlogClient.size() >= 1024) {
                    this.cacheTableMap4BinlogClient.clear();
                }
                return;
            case USER_VAR:
            case INTVAR:
            case RAND:
            case GTID:
            case PREVIOUS_GTIDS:
            case ANONYMOUS_GTID:
            case HEARTBEAT:
                return;
            default:
                return;
        }

        Event currentTableMapEvent;
        if (((TableMapEventData) this.tableMapEvent.getData()).getTableId() == dataTableId) {
            currentTableMapEvent = this.tableMapEvent;
        } else {
            Event e = this.cacheTableMapEvent4Transaction.get(dataTableId);
            Validate.notNull(e);
            currentTableMapEvent = e;
        }

        TableMapEventData tmed = currentTableMapEvent.getData();
        String dbTableName = tmed.getDatabase() + "." + tmed.getTable();

        BinlogData binlogData = feedOne(dbTableName);
        binlogData.setDbTableName(dbTableName);
        binlogData.setTablemap(currentTableMapEvent);
        binlogData.setEvent(event);
        binlogData.setTimestamp(currentTableMapEvent.getHeader().getTimestamp());

        byte[] columns = this.cacheColumnTypes.put(dbTableName, tmed.getColumnTypes());
        if (columns == null || !Arrays.equals(columns, tmed.getColumnTypes())) {
            binlogData.setReloadSchemaCache(true);
        }
        if (log.isDebugEnabled()) {
            log.debug(binlogData.toString());
        }
        push();

    }


    @Override public void onEvent(Event event) {

        this.iRingBuffer.add(Pair.of(this.mysqlClient.getBinlogFilename(), event), 1);
    }


    @Override public void onConnect(BinaryLogClient client) {

    }


    @Override public void onDisconnect(BinaryLogClient client) {

    }


    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception ex) {

        super.globalContext.fatalError("Mysql Communication Failure ", ex);
    }


    @Override
    public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {

        super.globalContext.fatalError("Mysql Deserialization Failure ", ex);
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
