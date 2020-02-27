package com.github.fevernova.task.mysql;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.task.mysql.data.ListData;
import lombok.extern.slf4j.Slf4j;


@Slf4j(topic = "fevernova-data")
public class JobParser extends AbstractParser<Long, ListData> {


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
    }


    @Override protected void handleEvent(Data event) {

        ListData oldData = (ListData) event;
        if (log.isDebugEnabled()) {
            log.debug(JSON.toJSONString(oldData));
        }
        ListData newData = feedOne(0L);
        newData.getValues().addAll(oldData.getValues());
        push();
    }
}
