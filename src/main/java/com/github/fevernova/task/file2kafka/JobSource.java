package com.github.fevernova.task.file2kafka;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.LogProxy;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.io.data.type.impl.UInteger;
import com.github.fevernova.io.kafka.data.KafkaData;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;


@Slf4j
public class JobSource extends AbstractSource<Integer, KafkaData> {


    private String filePath;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.filePath = taskContext.get("filepath");
    }


    @Override public void work() {

        final UInteger uInteger = new UInteger(false);

        try {
            Files.readLines(Paths.get(this.filePath).toFile(), Charset.defaultCharset(), new LineProcessor<String>() {


                @Override public boolean processLine(String line) throws IOException {

                    OrderCommand orderCommand = JSON.parseObject(line, OrderCommand.class);

                    if (LogProxy.LOG_DATA.isDebugEnabled()) {
                        LogProxy.LOG_DATA.debug(orderCommand.toString());
                    }

                    KafkaData kafkaData = feedOne(orderCommand.getSymbolId());
                    uInteger.from(orderCommand.getSymbolId());
                    kafkaData.setKey(uInteger.toBytes());
                    kafkaData.setValue(orderCommand.to());
                    kafkaData.setTimestamp(orderCommand.getTimestamp());
                    return true;
                }


                @Override public String getResult() {

                    return null;
                }
            });
        } catch (IOException e) {
            log.error("read file error : ", e);
        }
    }
}
