package com.github.fevernova.framework.service.state;


import com.github.fevernova.framework.component.ComponentType;
import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public class BinaryFileIdentity {


    private static final String SPLIT = "_";

    private ComponentType componentType;

    private int total;

    private int index;

    private String identity;

    private String cache;


    @Override public String toString() {

        if (this.cache == null) {
            StringBuilder sb = new StringBuilder();
            sb.append(this.componentType.name().toLowerCase()).append(SPLIT);
            sb.append(this.total).append(SPLIT);
            sb.append(this.index).append(SPLIT);
            sb.append(identity).append(SPLIT);
            this.cache = sb.toString();
        }
        return this.cache;
    }
}
