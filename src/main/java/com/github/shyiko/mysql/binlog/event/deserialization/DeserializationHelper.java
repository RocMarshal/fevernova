package com.github.shyiko.mysql.binlog.event.deserialization;


import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;


public class DeserializationHelper {


    public static Pair<EventDeserializer, Map<Long, TableMapEventData>> create() {

        Map<EventType, EventDataDeserializer> edds = new IdentityHashMap<>();
        Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<>();
        edds.put(EventType.FORMAT_DESCRIPTION, new FormatDescriptionEventDataDeserializer());
        edds.put(EventType.ROTATE, new RotateEventDataDeserializer());
        edds.put(EventType.INTVAR, new IntVarEventDataDeserializer());
        edds.put(EventType.QUERY, new QueryEventDataDeserializer());
        edds.put(EventType.TABLE_MAP, new TableMapEventDataDeserializer());
        edds.put(EventType.XID, new XidEventDataDeserializer());
        edds.put(EventType.WRITE_ROWS, new WriteRowsEventDataDeserializerV2(tableMapEventByTableId));
        edds.put(EventType.UPDATE_ROWS, new UpdateRowsEventDataDeserializerV2(tableMapEventByTableId));
        edds.put(EventType.DELETE_ROWS, new DeleteRowsEventDataDeserializerV2(tableMapEventByTableId));
        edds.put(EventType.EXT_WRITE_ROWS, new WriteRowsEventDataDeserializerV2(tableMapEventByTableId).setMayContainExtraInformation(true));
        edds.put(EventType.EXT_UPDATE_ROWS, new UpdateRowsEventDataDeserializerV2(tableMapEventByTableId).setMayContainExtraInformation(true));
        edds.put(EventType.EXT_DELETE_ROWS, new DeleteRowsEventDataDeserializerV2(tableMapEventByTableId).setMayContainExtraInformation(true));
        edds.put(EventType.ROWS_QUERY, new RowsQueryEventDataDeserializer());
        edds.put(EventType.GTID, new GtidEventDataDeserializer());
        edds.put(EventType.PREVIOUS_GTIDS, new PreviousGtidSetDeserializer());
        edds.put(EventType.XA_PREPARE, new XAPrepareEventDataDeserializer());
        EventDeserializer ed = new EventDeserializer(new EventHeaderV4Deserializer(), new NullEventDataDeserializer(), edds, tableMapEventByTableId);
        ed.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        return Pair.of(ed, tableMapEventByTableId);
    }

}
