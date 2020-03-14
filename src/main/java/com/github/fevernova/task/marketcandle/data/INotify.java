package com.github.fevernova.task.marketcandle.data;


import java.util.List;


public interface INotify {


    void onChange(Integer symbolId, List<Point> points);

}
