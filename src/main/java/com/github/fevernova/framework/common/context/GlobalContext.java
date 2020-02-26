package com.github.fevernova.framework.common.context;


import com.github.fevernova.framework.common.Util;
import com.google.common.eventbus.EventBus;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;


@Setter
@Getter
@Builder
@Slf4j
public class GlobalContext {


    private JobTags jobTags;

    private EventBus eventBus;

    private Map<String, Object> customContext;


    public void fatalError(String reason) {

        fatalError(reason, new RuntimeException());
    }


    public void fatalError(String reason, Throwable e) {

        if (e != null) {
            log.error("FatalError : " + reason, e);
        } else {
            log.error("FatalError : " + reason);
        }
        this.getEventBus().post(reason);
        while (true) {
            Util.sleepSec(1);
        }
    }
}
