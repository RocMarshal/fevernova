package com.github.fevernova.framework.common.structure.rb.adj;


import com.github.fevernova.framework.common.structure.rb.Element;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;
import java.util.List;


@Slf4j
@Builder
public class Adjustment4Test implements Adjustment {


    @Builder.Default
    private long intervalCycle = 10;

    @Builder.Default
    private long stepSize = 32;

    @Builder.Default
    private boolean roundTrip = true;

    private long ringBufferSize;

    private long ringBufferMinSize;

    private long ringBufferMaxSize;

    private final List<double[]> metrics = Lists.newArrayList();

    private final List<double[]> result = Lists.newArrayList();


    public void check() {

        Validate.isTrue(ringBufferSize >= ringBufferMaxSize);
        Validate.isTrue(ringBufferMaxSize >= ringBufferMinSize);
        Validate.isTrue(ringBufferMaxSize - ringBufferMinSize > stepSize);
    }


    @Override
    public long onEvent(Element current, Element next, long cycles, long sequenceInCycle, long flushSize) {

        if (cycles == 0) {
            return this.ringBufferMaxSize;
        } else if (sequenceInCycle == 0 && cycles > 0) {
            log.warn("Cycle {} , FlushSize {} , Cost {} ms , Volume {} , Record {} .", cycles, flushSize, current.getTimestampW
                    () - next.getTimestampW(), current.getAccVolume() - next.getAccVolume(), this.ringBufferSize);

            this.metrics.add(new double[] {flushSize, (current.getTimestampW() - next.getTimestampW()), ((double) this
                    .ringBufferSize * 1000) / (current.getTimestampW() - next.getTimestampW())});

            if (cycles % this.intervalCycle == 0) {
                evaluate();

                if (flushSize - this.ringBufferMinSize > this.stepSize) {
                    return flushSize - this.stepSize;
                } else {
                    this.result.forEach(doubles -> log.warn("evaluate : " + Arrays.toString(doubles)));
                    this.result.clear();
                    return this.roundTrip ? this.ringBufferMaxSize : flushSize;
                }
            }
        }
        return flushSize;
    }


    private void evaluate() {

        double totalRate = 0;
        for (int i = 0; i < this.metrics.size(); i++) {
            totalRate += this.metrics.get(i)[2];
        }
        double avgRate = totalRate / this.metrics.size();
        double acc = 0;
        for (int i = 0; i < this.metrics.size(); i++) {
            acc += Math.pow(this.metrics.get(i)[2] - avgRate, 2);
        }
        this.result.add(new double[] {this.metrics.get(0)[0], totalRate / this.metrics.size(), acc / (this.metrics.size() - 1)});
        this.metrics.clear();
    }
}
