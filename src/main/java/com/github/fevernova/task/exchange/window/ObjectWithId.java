package com.github.fevernova.task.exchange.window;


import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.jetbrains.annotations.NotNull;


@AllArgsConstructor
public abstract class ObjectWithId implements WriteBytesMarshallable, ReadBytesMarshallable, Comparable<ObjectWithId> {


    @Getter
    private int id;


    @Override public int compareTo(@NotNull ObjectWithId o) {

        return Integer.compare(this.id, o.getId());
    }

}
