package com.github.fevernova.framework.common.structure.rb.adj;


import com.github.fevernova.framework.common.structure.rb.Element;
import lombok.Builder;


@Builder
public class FixedRingBufferSize implements Adjustment {


    private long fixed;


    @Override
    public long onEvent(Element current, Element next, long cycles, long sequenceInCycle, long flushSize) {

        return this.fixed;
    }
}
