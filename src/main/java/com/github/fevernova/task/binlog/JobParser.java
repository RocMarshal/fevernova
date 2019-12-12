package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.task.binlog.data.MessageData;
import com.google.common.collect.Sets;

import java.util.Set;


public class JobParser extends AbstractParser<String, MessageData> {


    private Set<String> whiteList;


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.whiteList = Sets.newHashSet(Util.splitStringWithFilter(super.taskContext.get("whitelist"), "\\s", null));
    }


    @Override protected void handleEvent(Data event) {

    }
}
