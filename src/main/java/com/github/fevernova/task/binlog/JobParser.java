package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.task.binlog.data.BinlogData;
import com.github.fevernova.task.binlog.data.MessageData;
import com.github.fevernova.task.binlog.util.MysqlDataSource;
import com.github.fevernova.task.binlog.util.Table;
import com.google.common.collect.Sets;

import java.util.Set;


public class JobParser extends AbstractParser<String, MessageData> {


    private Set<String> whiteList;

    private MysqlDataSource mysql;


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.whiteList = Sets.newHashSet(Util.splitStringWithFilter(super.taskContext.get("whitelist"), "\\s", null));
    }


    @Override public void init() {

        super.init();
        this.mysql = (MysqlDataSource) super.globalContext.getCustomContext().get(MysqlDataSource.class.getSimpleName());
        if (super.index == 0) {
            this.mysql.config(this.whiteList, super.taskContext.getSubProperties("mapping."));
        }
    }


    @Override protected void handleEvent(Data event) {

        BinlogData binlogData = (BinlogData) event;
        if (!this.whiteList.contains(binlogData.getDbTableName())) {
            return;
        }

        Table table;
        if (binlogData.isReloadSchemaCache()) {
            table = this.mysql.reloadSchema(binlogData.getDbTableName());
        } else {
            table = this.mysql.getTable(binlogData.getDbTableName());
        }



    }
}
