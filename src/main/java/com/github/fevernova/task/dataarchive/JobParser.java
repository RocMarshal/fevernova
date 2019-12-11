package com.github.fevernova.task.dataarchive;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.data.Mapping;
import com.github.fevernova.data.message.DataContainer;
import com.github.fevernova.data.message.DataType;
import com.github.fevernova.data.message.Meta;
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
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;


@Slf4j
public class JobParser extends AbstractParser<byte[], ListData> {


    private final SerializerHelper serializer;

    private final List<ColumnInfo> columnInfos = Lists.newArrayList();

    private final List<Pair<ColumnInfo, Meta.MetaEntity>> handlers = Lists.newArrayList();

    private long currentMetaId = -1;


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        List<String> columns = Util.splitStringWithFilter(taskContext.getString("columns"), "\\s", null);
        TaskContext mappingContext = new TaskContext("mapping", taskContext.getSubProperties("mapping."));
        columns.forEach(s -> {
            TaskContext columnContext = new TaskContext("column-" + s, mappingContext.getSubProperties(s + "."));
            ColumnInfo columnInfo = ColumnInfo.builder()
                    .clazz(Util.findClass(columnContext.getString("udataclass")))
                    .sourceColumnName(columnContext.getString("sourcecolumnname"))
                    .fromType(Mapping.convert(DataType.valueOf(columnContext.getString("datatype"))))
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
        final DataContainer data = this.serializer.deserialize(null, kafkaData.getBytes());
        final ListData listData = feedOne(bizKey);
        if (listData.getValues() == null) {
            listData.setValues(Lists.newArrayListWithExpectedSize(this.columnInfos.size()));
        }

        if (this.currentMetaId != data.getMeta().getMetaId()) {
            this.handlers.clear();
            this.columnInfos.forEach(columnInfo -> {

                Meta.MetaEntity entity = data.getMeta().getEntity(columnInfo.getSourceColumnName());
                handlers.add(Pair.of(columnInfo, entity));
            });
        }
        this.handlers.forEach(handler -> data.get(handler.getValue(), (metaEntity, change, val, oldVal) -> listData.getValues().add(val)));
        push();
    }

}
