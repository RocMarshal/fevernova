package com.github.fevernova.framework.common;


import lombok.Builder;
import lombok.Getter;


@Builder
@Getter
public class Named {


    private static final String SPLIT = "-";

    private String taskName;

    private String moduleName;

    private String moduleType;

    private int index = 0;

    private int total = 0;

    private String val;


    public String render(boolean cache) {

        if (cache && this.val != null) {
            return this.val;
        }

        this.val = this.taskName + SPLIT + this.moduleName + SPLIT + this.moduleType;
        if (this.total > 0) {
            this.val = this.val + SPLIT + this.index + SPLIT + this.total;
        }

        return this.val;
    }

}
