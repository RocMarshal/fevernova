package com.github.fevernova.framework.common.structure.rb;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Optional;


@Getter
@Setter
@Builder
public class Element<T> {


    private Optional<T> body;

}
