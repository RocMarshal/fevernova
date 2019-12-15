package com.github.fevernova.framework.service.aligned;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.concurrent.CyclicBarrier;


@Slf4j
public class Aligner {


    private CyclicBarrier cyclicBarrier;

    private int parties;


    public Aligner(int p) {

        this.parties = p;
        this.cyclicBarrier = new CyclicBarrier(this.parties);
    }


    public void align() {

        if (this.parties > 1) {
            try {
                this.cyclicBarrier.await();
            } catch (Exception e) {
                log.error("align error : ", e);
                Validate.isTrue(false);
            }
        }
    }

}
