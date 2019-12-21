package com.github.fevernova.framework.service.state;


import com.github.fevernova.framework.component.ComponentType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@Setter
@ToString
public class StateValue {


    private ComponentType componentType;

    private int componentTotalNum;

    private int compomentIndex;

    private Object value;

}
