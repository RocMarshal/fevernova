package com.github.fevernova.framework.common.structure.rb;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Optional;


@Getter
@Setter
@Builder
public class Element<T> {

    private long sequence;

    private long timestampW;

    private long timestampR;

    private long accVolume;

    private Optional<T> body;

}
