package com.github.fevernova.task.mysql;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.io.mysql.data.ListData;
import lombok.extern.slf4j.Slf4j;
import org.mortbay.util.ajax.JSON;

import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class JobParser extends AbstractParser<Long, ListData> {


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
    }


    @Override protected void handleEvent(Data event) {

        ListData listData = (ListData) event;
        List<Object> result = listData.getValues().stream().map(columnObjectPair -> columnObjectPair.getValue()).collect(Collectors.toList());
        log.info(JSON.toString(result));
    }

}
