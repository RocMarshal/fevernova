package com.github.fevernova.framework.common.structure.rb;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@Builder
public class Element<T> {


    private T body;

}
