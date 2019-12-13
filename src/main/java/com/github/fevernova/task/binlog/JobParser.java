package com.github.fevernova.task.binlog;


import com.github.fevernova.data.message.DataContainer;
import com.github.fevernova.data.message.Meta;
import com.github.fevernova.data.message.Opt;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.task.binlog.data.BinlogData;
import com.github.fevernova.task.binlog.data.MessageData;
import com.github.fevernova.task.binlog.util.MysqlDataSource;
import com.github.fevernova.task.binlog.util.schema.Column;
import com.github.fevernova.task.binlog.util.schema.Table;
import com.github.shyiko.mysql.binlog.event.*;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.util.Map;
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

        Table table = binlogData.isReloadSchemaCache() ? this.mysql.reloadSchema(binlogData.getDbTableName()) :
                this.mysql.getTable(binlogData.getDbTableName());


        boolean theEnd = false;
        EventHeader eventHeader = binlogData.getEvent().getHeader();
        EventData eventData = binlogData.getEvent().getData();

        for (int i = 0; !theEnd; i++) {
            MessageData messageData = feedOne(binlogData.getDbTableName());
            messageData.setDestTopic(table.getTopic());
            messageData.setTimestamp(eventHeader.getTimestamp());
            StringBuilder bizKey = new StringBuilder(128);
            if (EventType.isWrite(eventHeader.getEventType())) {
                messageData.setDataContainer(DataContainer.createDataContainer4Write(table.getMeta(), Opt.INSERT));
                theEnd = parseWrite(i, (WriteRowsEventData) eventData, table, bizKey, messageData.getDataContainer());
            } else if (EventType.isUpdate(eventHeader.getEventType())) {
                messageData.setDataContainer(DataContainer.createDataContainer4Write(table.getMeta(), Opt.UPDATE));
                theEnd = parseUpdate(i, (UpdateRowsEventData) eventData, table, bizKey, messageData.getDataContainer());
            } else if (EventType.isDelete(eventHeader.getEventType())) {
                messageData.setDataContainer(DataContainer.createDataContainer4Write(table.getMeta(), Opt.DELETE));
                theEnd = parseDelete(i, (DeleteRowsEventData) eventData, table, bizKey, messageData.getDataContainer());
            } else {
                Validate.isTrue(false, "Event Type Error : " + eventHeader.getEventType());
            }
            messageData.setKey(bizKey.toString().getBytes());
            push();
        }
    }


    private boolean parseWrite(int index, WriteRowsEventData event, Table table, StringBuilder bizKey, DataContainer container) {

        Serializable[] row = event.getRows().get(index);
        int colSize = Math.min(table.getColumns().size(), row.length);
        for (int i = 0; i < colSize; i++) {
            if (event.getIncludedColumns().get(i)) {
                Column column = table.getColumns().get(i);
                Meta.MetaEntity metaEntity = table.getMeta().getEntity(i);
                if (!column.isIgnore()) {
                    column.getUData().from(row[i], column.getFrom());
                    container.put(metaEntity, column.getUData().to(column.getTo()));
                    if (column.isPrimaryKey()) {
                        if (bizKey.length() > 0) {
                            bizKey.append("\u0001");
                        }
                        bizKey.append(row[i]);
                    }
                }
            }
        }
        return event.getRows().size() - 1 == index;
    }


    private boolean parseUpdate(int index, UpdateRowsEventData event, Table table, StringBuilder bizKey, DataContainer container) {

        Map.Entry<Serializable[], Serializable[]> row = event.getRows().get(index);
        int colSize = Math.min(table.getColumns().size(), row.getValue().length);
        for (int i = 0; i < colSize; i++) {
            if (event.getIncludedColumns().get(i)) {
                Column column = table.getColumns().get(i);
                Meta.MetaEntity metaEntity = table.getMeta().getEntity(i);
                if (!column.isIgnore()) {
                    column.getUData().from(row.getValue()[i], column.getFrom());
                    Object cur = column.getUData().to(column.getTo());
                    if (event.getIncludedColumnsBeforeUpdate().get(i)) {
                        column.getUData().from(row.getKey()[i], column.getFrom());
                        container.put(metaEntity, cur, column.getUData().to(column.getTo()));
                    } else {
                        container.put(metaEntity, cur);
                    }
                    if (column.isPrimaryKey()) {
                        if (bizKey.length() > 0) {
                            bizKey.append("\u0001");
                        }
                        bizKey.append(row.getValue()[i]);
                    }
                }
            }
        }
        return event.getRows().size() - 1 == index;
    }


    private boolean parseDelete(int index, DeleteRowsEventData event, Table table, StringBuilder bizKey, DataContainer container) {

        Serializable[] row = event.getRows().get(index);
        int colSize = Math.min(table.getColumns().size(), row.length);
        for (int i = 0; i < colSize; i++) {
            if (event.getIncludedColumns().get(i)) {
                Column column = table.getColumns().get(i);
                Meta.MetaEntity metaEntity = table.getMeta().getEntity(i);
                if (!column.isIgnore()) {
                    column.getUData().from(row[i], column.getFrom());
                    container.put(metaEntity, column.getUData().to(column.getTo()));
                    if (column.isPrimaryKey()) {
                        if (bizKey.length() > 0) {
                            bizKey.append("\u0001");
                        }
                        bizKey.append(row[i]);
                    }
                }
            }
        }
        return event.getRows().size() - 1 == index;
    }
}
