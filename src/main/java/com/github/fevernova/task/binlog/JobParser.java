package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.io.data.TypeRouter;
import com.github.fevernova.io.data.message.DataContainer;
import com.github.fevernova.io.data.message.Meta;
import com.github.fevernova.io.data.message.Opt;
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
        if (isFirst()) {
            this.mysql.config(this.whiteList, super.taskContext.getSubProperties("mapping."));
        }
    }


    @Override protected void handleEvent(Data event) {

        BinlogData binlogData = (BinlogData) event;
        if (!this.whiteList.contains(binlogData.getDbTableName())) {
            return;
        }

        Table table = this.mysql.getTable(binlogData.getDbTableName(), binlogData.isReloadSchemaCache());
        EventHeader eventHeader = binlogData.getEvent().getHeader();
        EventData eventData = binlogData.getEvent().getData();

        for (int i = 0; i < binlogData.getRowsNum(); i++) {
            MessageData messageData = feedOne(binlogData.getDbTableName());
            messageData.setDestTopic(table.getTopic());
            messageData.setTimestamp(eventHeader.getTimestamp());
            StringBuilder bizKey = new StringBuilder(128);
            if (EventType.isWrite(eventHeader.getEventType())) {
                messageData.setDataContainer(DataContainer.createDataContainer4Write(table.getMeta(), Opt.INSERT));
                parseWrite(i, (WriteRowsEventData) eventData, table, bizKey, messageData.getDataContainer());
            } else if (EventType.isUpdate(eventHeader.getEventType())) {
                messageData.setDataContainer(DataContainer.createDataContainer4Write(table.getMeta(), Opt.UPDATE));
                parseUpdate(i, (UpdateRowsEventData) eventData, table, bizKey, messageData.getDataContainer());
            } else if (EventType.isDelete(eventHeader.getEventType())) {
                messageData.setDataContainer(DataContainer.createDataContainer4Write(table.getMeta(), Opt.DELETE));
                parseDelete(i, (DeleteRowsEventData) eventData, table, bizKey, messageData.getDataContainer());
            } else {
                Validate.isTrue(false, "Event Type Error : " + eventHeader.getEventType());
            }
            messageData.setKey(bizKey.toString().getBytes());
            DataContainer dc = messageData.getDataContainer();
            dc.putTag("dbtable", table.getDbTableName());
            dc.setTimestamp(eventHeader.getTimestamp());
            dc.setNid(binlogData.getGlobalId() + i);
            dc.writeFinished();
            push();
        }
    }


    private void parseWrite(int index, WriteRowsEventData event, Table table, StringBuilder bizKey, DataContainer container) {

        Serializable[] row = event.getRows().get(index);
        int colSize = Math.min(table.getColumns().size(), row.length);
        for (int i = 0; i < colSize; i++) {
            if (event.getIncludedColumns().get(i)) {
                Column column = table.getColumns().get(i);
                Meta.MetaEntity metaEntity = table.getMeta().getEntity(i);
                if (!column.isIgnore()) {
                    TypeRouter typeRouter = column.getTypeRouter();
                    container.put(metaEntity, typeRouter.getUData().from(row[i], typeRouter.getFrom()).to(typeRouter.getTo()));
                    if (column.isPrimaryKey()) {
                        if (bizKey.length() > 0) {
                            bizKey.append("\u0001");
                        }
                        bizKey.append(row[i]);
                    }
                }
            }
        }
    }


    private void parseUpdate(int index, UpdateRowsEventData event, Table table, StringBuilder bizKey, DataContainer container) {

        Map.Entry<Serializable[], Serializable[]> row = event.getRows().get(index);
        int colSize = Math.min(table.getColumns().size(), row.getValue().length);
        for (int i = 0; i < colSize; i++) {
            if (event.getIncludedColumns().get(i)) {
                Column column = table.getColumns().get(i);
                if (!column.isIgnore()) {
                    Meta.MetaEntity metaEntity = table.getMeta().getEntity(i);
                    TypeRouter typeRouter = column.getTypeRouter();
                    Object cur = typeRouter.getUData().from(row.getValue()[i], typeRouter.getFrom()).to(typeRouter.getTo());
                    if (event.getIncludedColumnsBeforeUpdate().get(i)) {
                        Object old = typeRouter.getUData().from(row.getKey()[i], typeRouter.getFrom()).to(typeRouter.getTo());
                        container.put(metaEntity, cur, old);
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
    }


    private void parseDelete(int index, DeleteRowsEventData event, Table table, StringBuilder bizKey, DataContainer container) {

        Serializable[] row = event.getRows().get(index);
        int colSize = Math.min(table.getColumns().size(), row.length);
        for (int i = 0; i < colSize; i++) {
            if (event.getIncludedColumns().get(i)) {
                Column column = table.getColumns().get(i);
                Meta.MetaEntity metaEntity = table.getMeta().getEntity(i);
                if (!column.isIgnore()) {
                    TypeRouter typeRouter = column.getTypeRouter();
                    container.put(metaEntity, typeRouter.getUData().from(row[i], typeRouter.getFrom()).to(typeRouter.getTo()));
                    if (column.isPrimaryKey()) {
                        if (bizKey.length() > 0) {
                            bizKey.append("\u0001");
                        }
                        bizKey.append(row[i]);
                    }
                }
            }
        }
    }
}
