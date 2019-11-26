package com.github.fevernova.framework.metric;


import com.codahale.metrics.Counter;
import com.github.fevernova.framework.common.Named;
import com.github.fevernova.framework.component.ComponentType;
import lombok.Getter;


public class UnitCounter extends Counter {


    @Getter
    private ComponentType componentType;

    @Getter
    private String name;

    @Getter
    private String unit;

    private long ratio = 1L;

    private long mark;


    public UnitCounter(ComponentType componentType, String name, String unit) {

        super();
        this.name = name;
        this.unit = unit;
        this.componentType = componentType;
    }


    public UnitCounter(ComponentType componentType, String name, String unit, long ratio) {

        this(componentType, name, unit);
        this.ratio = ratio;
    }


    @Override
    public long getCount() {

        return super.getCount() / this.ratio;
    }


    public long getCountAndReset() {

        long c = super.getCount();
        long r = c - this.mark;
        this.mark = c;
        return r / this.ratio;
    }


    public String getRegisterName(Named named) {

        return named.render(true) + "-" + name;
    }

}
