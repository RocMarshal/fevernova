package com.github.fevernova.task.marketcandle;


import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.framework.service.checkpoint.ICheckPointSaver;
import com.github.fevernova.framework.service.checkpoint.MapCheckPoint;
import com.github.fevernova.framework.service.state.BinaryFileIdentity;
import com.github.fevernova.framework.service.state.StateService;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.framework.task.Manager;
import com.github.fevernova.io.kafka.data.KafkaData;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.OrderMatchFactory;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.marketcandle.data.CandleData;
import com.github.fevernova.task.marketcandle.data.CandleDiff;
import com.github.fevernova.task.marketcandle.data.INotify;
import com.github.fevernova.task.marketcandle.data.Point;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.List;


@Slf4j
public class JobParser extends AbstractParser<Integer, CandleDiff> implements BarrierCoordinatorListener, INotify {


    private ICheckPointSaver<MapCheckPoint> checkpoints = new CheckPointSaver<>();

    private OrderMatch orderMatch = (OrderMatch) new OrderMatchFactory().createData();

    private CandleData candleData = new CandleData();

    private BinaryFileIdentity candleDataIdentity;

    private long lastScanTime = Util.nowMS();

    private long lastMsgTime;

    private long interval;

    private long maxDelayRepair;


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.candleDataIdentity = BinaryFileIdentity.builder().componentType(super.componentType).total(super.total).index(super.index)
                .identity(CandleData.CONS_NAME.toLowerCase()).build();
        this.interval = taskContext.getLong("interval", 2000L);
        this.maxDelayRepair = taskContext.getLong("maxdelayrepair", 60 * 1000L);
    }


    @Override protected void handleEvent(Data event) {

        KafkaData kafkaData = (KafkaData) event;
        this.orderMatch.from(kafkaData.getValue());
        this.lastMsgTime = this.orderMatch.getTimestamp();
        if (ResultCode.MATCH == this.orderMatch.getResultCode()) {
            this.candleData.handle(this.orderMatch, this);
        }
        flush();
    }


    @Override protected void timeOut() {

        super.timeOut();
        flush();
    }


    private void flush() {

        long ts = Util.nowMS();
        if (ts - this.lastScanTime < this.interval) {
            return;
        }
        boolean repair = (ts - this.lastMsgTime) < this.maxDelayRepair;
        this.lastScanTime = ts;
        this.candleData.scan4Update(repair, this, ts);
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

        super.snapshotWhenBarrier(barrierData);
        MapCheckPoint checkPoint = new MapCheckPoint();
        StateService stateService = Manager.getInstance().getStateService();
        if (stateService.isSupportRecovery()) {
            String path = stateService.saveBinary(this.candleDataIdentity, barrierData, this.candleData);
            checkPoint.getValues().put(this.candleDataIdentity.getIdentity(), path);
        }
        this.checkpoints.put(barrierData.getBarrierId(), checkPoint);
        flush();
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return this.checkpoints.getCheckPoint(barrierData.getBarrierId()) != null;
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) throws Exception {

        MapCheckPoint checkPoint = this.checkpoints.getCheckPoint(barrierData.getBarrierId());
        StateValue stateValue = new StateValue();
        stateValue.setComponentType(super.componentType);
        stateValue.setComponentTotalNum(super.total);
        stateValue.setCompomentIndex(super.index);
        stateValue.setValue(checkPoint);
        return stateValue;
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        Validate.isTrue(result);
    }


    @Override public void onRecovery(List<StateValue> stateValueList) {

        super.onRecovery(stateValueList);
        stateValueList.forEach(stateValue -> {
            if (stateValue.getCompomentIndex() == index) {
                MapCheckPoint checkPoint = new MapCheckPoint();
                checkPoint.parseFromJSON((JSONObject) stateValue.getValue());
                Manager.getInstance().getStateService().recoveryBinary(checkPoint.getValues().get(candleDataIdentity.getIdentity()), candleData);
            }
        });
    }


    @Override public boolean needRecovery() {

        return true;
    }


    @Override public void onChange(Integer symbolId, List<Point> points) {

        CandleDiff candleDiff = feedOne(symbolId);
        candleDiff.setSymbolId(symbolId);
        candleDiff.setDiff(points);
        push();
    }
}
