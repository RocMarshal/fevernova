package com.github.fevernova.task.binlog;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.LogProxy;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.structure.rb.IRingBuffer;
import com.github.fevernova.framework.common.structure.rb.SimpleRingBuffer;
import com.github.fevernova.framework.component.ComponentStatus;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.framework.service.checkpoint.ICheckPointSaver;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.task.binlog.data.BinlogData;
import com.github.fevernova.task.binlog.data.MysqlCheckPoint;
import com.github.fevernova.task.binlog.util.MysqlDataSource;
import com.github.fevernova.task.binlog.util.SimpleBinlogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.DeserializationHelper;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class JobSource extends AbstractSource<String, BinlogData>
        implements BinaryLogClient.EventListener, BinaryLogClient.LifecycleListener, BarrierCoordinatorListener {


    private final ICheckPointSaver<MysqlCheckPoint> checkpoints;

    private final TaskContext dataSourceContext;

    private final MysqlDataSource mysqlDataSource;

    private final BinaryLogClient mysqlClient;

    private final IRingBuffer<Pair<String, Event>> iRingBuffer;

    //cache
    private Event tableMapEvent = null;

    private Map<String, byte[]> cacheColumnTypes = Maps.newHashMap();

    private Map<Long, Event> cacheTableMapEvent4Transaction = Maps.newHashMap();

    private Map<Long, TableMapEventData> cacheTableMap4BinlogClient;

    //use for checkpoint
    private String binlogFileName;

    private long binlogPosition;

    private long binlogTimestamp;

    private long globalId = 0;


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
        this.iRingBuffer = new SimpleRingBuffer<>(taskContext.getInteger("buffersize", 128));
        super.globalContext.getCustomContext().put(MysqlDataSource.class.getSimpleName(), this.mysqlDataSource);

        this.binlogFileName = taskContext.get("binlogfilename");
        this.binlogPosition = taskContext.getLong("binlogposition", 0L);
        this.mysqlClient.setBinlogFilename(this.binlogFileName);
        this.mysqlClient.setBinlogPosition(this.binlogPosition);
    }


    @Override public void onStart() {

        super.onStart();
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
        int rowsNum = 0;
        switch (eventType) {
            case PRE_GA_WRITE_ROWS:
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                dataTableId = ((WriteRowsEventData) event.getData()).getTableId();
                rowsNum = ((WriteRowsEventData) event.getData()).getRows().size();
                break;
            case PRE_GA_UPDATE_ROWS:
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                dataTableId = ((UpdateRowsEventData) event.getData()).getTableId();
                rowsNum = ((UpdateRowsEventData) event.getData()).getRows().size();
                break;
            case PRE_GA_DELETE_ROWS:
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                dataTableId = ((DeleteRowsEventData) event.getData()).getTableId();
                rowsNum = ((DeleteRowsEventData) event.getData()).getRows().size();
                break;

            case TABLE_MAP:
                dataTableId = ((TableMapEventData) event.getData()).getTableId();
                if (this.tableMapEvent == null) {
                    this.tableMapEvent = event;
                    this.binlogFileName = tmpFileName;
                    this.binlogPosition = ((EventHeaderV4) this.tableMapEvent.getHeader()).getPosition();
                    this.binlogTimestamp = this.tableMapEvent.getHeader().getTimestamp();
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
                this.binlogFileName = tmpFileName;
                this.binlogPosition = ((EventHeaderV4) event.getHeader()).getNextPosition();
                this.binlogTimestamp = event.getHeader().getTimestamp();
                return;

            case ROTATE:
            case HEARTBEAT:
            case FORMAT_DESCRIPTION:
                return;

            case QUERY:
            case ROWS_QUERY:
                //TODO DDL处理
                this.binlogFileName = tmpFileName;
                this.binlogPosition = ((EventHeaderV4) event.getHeader()).getNextPosition();
                this.binlogTimestamp = event.getHeader().getTimestamp();
                return;

            case USER_VAR:
            case INTVAR:
            case RAND:
                log.error("Illegal event : " + event.toString());
                this.binlogFileName = tmpFileName;
                this.binlogPosition = ((EventHeaderV4) event.getHeader()).getNextPosition();
                this.binlogTimestamp = event.getHeader().getTimestamp();
                return;

            case GTID:
            case PREVIOUS_GTIDS:
            case ANONYMOUS_GTID:
            case TRANSACTION_CONTEXT:
            case VIEW_CHANGE:
                //TODO 处理GTID
                this.binlogFileName = tmpFileName;
                this.binlogPosition = ((EventHeaderV4) event.getHeader()).getNextPosition();
                this.binlogTimestamp = event.getHeader().getTimestamp();
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
        binlogData.setGlobalId(this.globalId);
        binlogData.setRowsNum(rowsNum);

        byte[] columns = this.cacheColumnTypes.put(dbTableName, tmed.getColumnTypes());
        if (columns == null || !Arrays.equals(columns, tmed.getColumnTypes())) {
            binlogData.setReloadSchemaCache(true);
        }
        if (LogProxy.LOG_DATA.isDebugEnabled()) {
            LogProxy.LOG_DATA.debug(binlogData.toString());
        }
        push();
        this.globalId = this.globalId + rowsNum;
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

        MysqlCheckPoint mysqlCheckPoint = MysqlCheckPoint.builder()
                .host(this.mysqlDataSource.getHost())
                .port(this.mysqlDataSource.getPort())
                .serverId(this.mysqlDataSource.getServerId())
                .binlogFileName(this.binlogFileName)
                .binlogPosition(this.binlogPosition)
                .binlogTimestamp(this.binlogTimestamp)
                .globalId(this.globalId)
                .build();
        this.checkpoints.put(barrierData.getBarrierId(), mysqlCheckPoint);
    }


    @Override public void onEvent(Event event) {

        Pair<String, Event> x = Pair.of(this.mysqlClient.getBinlogFilename(), event);
        int k = 0;
        while (!this.iRingBuffer.add(x, 1)) {
            if (super.status == ComponentStatus.CLOSING) {
                return;
            }
            if (k++ > 10) {
                Util.sleepMS(1);
                k = 0;
            }
        }
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
                log.error("mysql client closed .");
            }
        } catch (Exception e) {
            log.error("Source shutdown error : ", e);
        }
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return this.checkpoints.getCheckPoint(barrierData.getBarrierId()) != null;
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) {

        MysqlCheckPoint mysqlCheckPoint = this.checkpoints.getCheckPoint(barrierData.getBarrierId());
        StateValue stateValue = new StateValue();
        stateValue.setComponentType(super.componentType);
        stateValue.setComponentTotalNum(super.total);
        stateValue.setCompomentIndex(super.index);
        stateValue.setValue(Maps.newHashMap());
        stateValue.getValue().put("mysql", JSON.toJSONString(mysqlCheckPoint));
        return stateValue;
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        Validate.isTrue(result);
        MysqlCheckPoint mysqlCheckPoint = this.checkpoints.remove(barrierData.getBarrierId());
        if (log.isInfoEnabled()) {
            log.info("commit checkpoint : " + mysqlCheckPoint.toString());
        }
    }


    @Override public void onRecovery(List<StateValue> stateValues) {

        super.onRecovery(stateValues);
        StateValue stateValue = stateValues.get(0);
        log.info("match state : " + stateValue.getValue().get("mysql"));
        MysqlCheckPoint cp = JSON.parseObject(stateValue.getValue().get("mysql"), MysqlCheckPoint.class);
        this.binlogTimestamp = cp.getBinlogTimestamp();
        this.globalId = cp.getGlobalId();
        if (this.mysqlDataSource.getServerId() == cp.getServerId()) {
            log.info("the serverid as same as last checkpoint , Go on : " + cp.getBinlogFileName() + "/" + cp.getBinlogPosition());
            this.binlogFileName = cp.getBinlogFileName();
            this.binlogPosition = cp.getBinlogPosition();
        } else {
            log.warn("Sid is diff (old:" + cp.getServerId() + " new: " + this.mysqlDataSource.getServerId() + ")");
            SimpleBinlogClient sbc = new SimpleBinlogClient(this.mysqlDataSource.getHost(),
                                                            this.mysqlDataSource.getPort(),
                                                            this.mysqlDataSource.getUsername(),
                                                            this.mysqlDataSource.getPassword(),
                                                            this.mysqlDataSource.getSlaveId(),
                                                            cp.getBinlogTimestamp() - super.taskContext.getLong("rollback", 60000L));
            try {
                sbc.connect();
                Validate.isTrue(sbc.getBinlogFileName() != null, "auto fetch failed : not found");
                Validate.isTrue(cp.getBinlogTimestamp() - sbc.getLastDBTime() < super.taskContext.getLong("tolerate", 5 * 60000L),
                                "auto fetch failed by delay : " + (cp.getBinlogTimestamp() - sbc.getLastDBTime()));
                this.binlogFileName = sbc.getBinlogFileName();
                this.binlogPosition = sbc.getBinlogPosition();
                log.warn("auto fetch result : " + sbc.getBinlogFileName() + "/" + sbc.getBinlogPosition());
            } catch (Exception e) {
                log.error("auto fetch failed :", e);
                Validate.isTrue(false);
            }
        }
        this.mysqlClient.setBinlogFilename(this.binlogFileName);
        this.mysqlClient.setBinlogPosition(this.binlogPosition);
    }
}
