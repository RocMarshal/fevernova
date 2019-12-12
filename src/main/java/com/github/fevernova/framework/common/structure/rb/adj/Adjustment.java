package com.github.fevernova.framework.common.structure.rb.adj;


import com.github.fevernova.framework.common.structure.rb.Element;


public interface Adjustment {


    long onEvent(Element current, Element next, long cycles, long sequenceInCycle, long flushSize);

}
