package com.github.fevernova.task.dataarchive;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.data.message.DataContainer;
import com.github.fevernova.data.message.SerializerHelper;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.common.data.list.ListData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.framework.schema.ColumnInfo;
import com.github.fevernova.framework.schema.SchemaData;
import com.github.fevernova.kafka.data.KafkaData;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
public class JobParser extends AbstractParser<byte[], ListData> {


    private SerializerHelper serializer;

    private List<ColumnInfo> columnInfos = Lists.newArrayList();


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        List<String> columns = Util.splitStringWithFilter(taskContext.getString("columns"), "\\s", null);
        TaskContext mappingContext = new TaskContext("mapping", taskContext.getSubProperties("mapping."));
        columns.forEach(s -> {
            TaskContext columnContext = new TaskContext("column-" + s, mappingContext.getSubProperties(s + "."));
            ColumnInfo columnInfo = ColumnInfo.builder()
                    .clazz(Util.findClass(columnContext.getString("class")))
                    .sourceColumnName(columnContext.getString("sourcecolumnname"))
                    //TODO .fromType(columnContext.getString("mysqltype"))
                    .targetColumnName(columnContext.getString("targetcolumnname"))
                    .targetTypeEnum(columnContext.getString("targettypeenum"))
                    .build();
            this.columnInfos.add(columnInfo);
        });

        this.serializer = new SerializerHelper();
        if (log.isInfoEnabled()) {
            log.info("column infos : " + JSON.toJSONString(this.columnInfos, true));
        }
    }


    @Override protected BroadcastData onBroadcast(BroadcastData broadcastData) {

        return new SchemaData(this.columnInfos);
    }


    @Override protected void handleEvent(Data event) {

        KafkaData kafkaData = (KafkaData) event;
        byte[] bizKey = kafkaData.getKey();
        DataContainer data = this.serializer.deserialize(null, kafkaData.getBytes());
        ListData listData = feedOne(bizKey);
        List<Object> result = listData.getValues();
        if (result == null) {
            result = Lists.newArrayListWithExpectedSize(this.columnInfos.size());
            listData.setValues(result);
        }
        this.columnInfos.forEach(columnInfo -> data.get(columnInfo.getSourceColumnName(), null));
        push();
    }

}
