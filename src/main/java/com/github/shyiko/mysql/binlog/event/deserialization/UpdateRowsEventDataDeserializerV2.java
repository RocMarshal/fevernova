package com.github.shyiko.mysql.binlog.event.deserialization;


import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public class UpdateRowsEventDataDeserializerV2 extends AbstractRowsEventDataDeserializerV2<UpdateRowsEventData> {


    private boolean mayContainExtraInformation;


    public UpdateRowsEventDataDeserializerV2(Map<Long, TableMapEventData> tableMapEventByTableId) {

        super(tableMapEventByTableId);
    }


    public UpdateRowsEventDataDeserializerV2 setMayContainExtraInformation(boolean mayContainExtraInformation) {

        this.mayContainExtraInformation = mayContainExtraInformation;
        return this;
    }


    @Override
    public UpdateRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {

        UpdateRowsEventData eventData = new UpdateRowsEventData();
        eventData.setTableId(inputStream.readLong(6));
        inputStream.skip(2); // reserved
        if (mayContainExtraInformation) {
            int extraInfoLength = inputStream.readInteger(2);
            inputStream.skip(extraInfoLength - 2);
        }
        int numberOfColumns = inputStream.readPackedInteger();
        eventData.setIncludedColumnsBeforeUpdate(inputStream.readBitSet(numberOfColumns, true));
        eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
        eventData.setRows(deserializeRows(eventData, inputStream));
        return eventData;
    }


    private List<Map.Entry<Serializable[], Serializable[]>> deserializeRows(UpdateRowsEventData eventData,
                                                                            ByteArrayInputStream inputStream) throws IOException {

        long tableId = eventData.getTableId();
        BitSet includedColumnsBeforeUpdate = eventData.getIncludedColumnsBeforeUpdate(),
                includedColumns = eventData.getIncludedColumns();
        List<Map.Entry<Serializable[], Serializable[]>> rows =
                new ArrayList<Map.Entry<Serializable[], Serializable[]>>();
        while (inputStream.available() > 0) {
            rows.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(
                    deserializeRow(tableId, includedColumnsBeforeUpdate, inputStream),
                    deserializeRow(tableId, includedColumns, inputStream)
            ));
        }
        return rows;
    }

}
