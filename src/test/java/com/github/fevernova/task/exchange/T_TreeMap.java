package com.github.fevernova.task.exchange;


import com.github.fevernova.Common;
import com.github.fevernova.framework.common.Util;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.NavigableMap;


public class T_TreeMap {


    NavigableMap<Long, String> tree = Maps.newTreeMap();


    public void loadDepth(long depth) {

        for (long i = 0L; i < depth; i++) {
            tree.put(i, i + "");
        }
    }


    @Test
    public void T_performance() {

        loadDepth(5000);
        Common.warn();
        long r = 0L;
        long st = Util.nowMS();
        for (int i = 0; i < 10_0000_0000; i++) {
            String x = tree.get(100L);
            r = (x == null ? r : r + 1);
        }
        long et = Util.nowMS();
        System.out.println(et - st);
    }

}
