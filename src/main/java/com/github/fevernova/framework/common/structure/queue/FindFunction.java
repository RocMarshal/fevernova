package com.github.fevernova.framework.common.structure.queue;


public interface FindFunction<E extends LinkedObject<E>> {


    boolean find(E e);

}
