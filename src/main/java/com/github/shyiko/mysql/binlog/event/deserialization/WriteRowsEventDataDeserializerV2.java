package com.github.shyiko.mysql.binlog.event.deserialization;


import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class WriteRowsEventDataDeserializerV2 extends AbstractRowsEventDataDeserializerV2<WriteRowsEventData> {


    private boolean mayContainExtraInformation;


    public WriteRowsEventDataDeserializerV2(Map<Long, TableMapEventData> tableMapEventByTableId) {

        super(tableMapEventByTableId);
    }


    public WriteRowsEventDataDeserializerV2 setMayContainExtraInformation(boolean mayContainExtraInformation) {

        this.mayContainExtraInformation = mayContainExtraInformation;
        return this;
    }


    @Override
    public WriteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {

        WriteRowsEventData eventData = new WriteRowsEventData();
        eventData.setTableId(inputStream.readLong(6));
        inputStream.skip(2); // reserved
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

        List<Serializable[]> result = new LinkedList<Serializable[]>();
        while (inputStream.available() > 0) {
            result.add(deserializeRow(tableId, includedColumns, inputStream));
        }
        return result;
    }

}

