package com.github.fevernova.framework.common.data;


public interface DataFactory<T extends Data> {


    T createData();
}
