package com.github.fevernova.framework.common.structure.queue;


import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
public abstract class LinkedObject<E extends LinkedObject> {


    private E next;


}
