package com.github.fevernova.task.exchange;


import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.NavigableMap;


public class T_TreeMap {


    private NavigableMap<Long, String> tree0 = Maps.newTreeMap(Long::compareTo);

    private NavigableMap<Long, String> tree1 = Maps.newTreeMap((l1, l2) -> l2.compareTo(l1));


    @Test
    public void T_treemap() {

        for (long i = 1L; i < 10; i++) {
            tree0.put(i, i + "");
        }
        System.out.println(tree0.ceilingEntry(0L));
        for (long i = 1L; i < 10; i++) {
            tree1.put(i, i + "");
        }
        System.out.println(tree1.ceilingEntry(20L));
    }

}
