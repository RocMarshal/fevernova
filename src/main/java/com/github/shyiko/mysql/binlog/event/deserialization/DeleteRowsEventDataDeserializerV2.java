package com.github.shyiko.mysql.binlog.event.deserialization;


import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;


public class DeleteRowsEventDataDeserializerV2 extends AbstractRowsEventDataDeserializerV2<DeleteRowsEventData> {


    private boolean mayContainExtraInformation;


    public DeleteRowsEventDataDeserializerV2(Map<Long, TableMapEventData> tableMapEventByTableId) {

        super(tableMapEventByTableId);
    }


    public DeleteRowsEventDataDeserializerV2 setMayContainExtraInformation(boolean mayContainExtraInformation) {

        this.mayContainExtraInformation = mayContainExtraInformation;
        return this;
    }


    @Override
    public DeleteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {

        DeleteRowsEventData eventData = new DeleteRowsEventData();
        eventData.setTableId(inputStream.readLong(6));
        inputStream.readInteger(2); // reserved
        if (mayContainExtraInformation) {
            int extraInfoLength = inputStream.readInteger(2);
            inputStream.skip(extraInfoLength - 2);
        }
        int numberOfColumns = inputStream.readPackedInteger();
        eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
        eventData.setRows(deserializeRows(eventData.getTableId(), eventData.getIncludedColumns(), inputStream));
        return eventData;
    }


    private List<Serializable[]> deserializeRows(long tableId, BitSet includedColumns, ByteArrayInputStream inputStream)
            throws IOException {

        List<Serializable[]> result = Lists.newArrayListWithCapacity(2);
        while (inputStream.available() > 0) {
            result.add(deserializeRow(tableId, includedColumns, inputStream));
        }
        return result;
    }

}
