package com.github.fevernova.hdfs;


import java.util.TreeSet;


public class Constants {


    public static final String DAY_HOUR = "DayHour";

    public static final String DAY_HOUR_MINUTE = "DayHourMinute";

    public static final TreeSet<Integer> MINUTE_PERIOD_SET = new TreeSet<>();

    public static final TreeSet<Integer> HOUR_PERIOD_SET = new TreeSet<>();

    static {
        MINUTE_PERIOD_SET.add(1);
        MINUTE_PERIOD_SET.add(2);
        MINUTE_PERIOD_SET.add(3);
        MINUTE_PERIOD_SET.add(4);
        MINUTE_PERIOD_SET.add(5);
        MINUTE_PERIOD_SET.add(6);
        MINUTE_PERIOD_SET.add(10);
        MINUTE_PERIOD_SET.add(12);
        MINUTE_PERIOD_SET.add(15);
        MINUTE_PERIOD_SET.add(20);
        MINUTE_PERIOD_SET.add(30);
        HOUR_PERIOD_SET.add(1);
        HOUR_PERIOD_SET.add(2);
        HOUR_PERIOD_SET.add(3);
        HOUR_PERIOD_SET.add(4);
        HOUR_PERIOD_SET.add(6);
        HOUR_PERIOD_SET.add(8);
        HOUR_PERIOD_SET.add(12);
    }

}
