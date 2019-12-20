package com.github.fevernova.uniq;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.service.uniq.SlideWindowFilter;
import com.github.fevernova.framework.service.uniq.Util;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class T_SlideWindowFilter {


    private SlideWindowFilter filter;


    @Before
    public void init() {

        GlobalContext globalContext = GlobalContext.builder().build();
        TaskContext taskContext = new TaskContext("uniq");
        this.filter = new SlideWindowFilter(globalContext, taskContext);
    }


    @Test
    public void T_base() {

        long time = System.currentTimeMillis();
        Assert.assertTrue(this.filter.uniq(123, 9876543210L, time));
        Assert.assertFalse(this.filter.uniq(123, 9876543210L, time));
        Assert.assertTrue(this.filter.uniq(123, 9876543210L, time + 60000));
    }


    @Test
    public void T_snapshot() throws IOException {


        for (long i = 0; i < 100000000; i++) {
            this.filter.uniq(1, i * 2, 123123);
        }
        Util.saveData("/tmp/plp/123", this.filter);
    }


    @Test
    public void T_reload() {

        init();
        Util.loadData("/tmp/plp/123", this.filter);
    }
}
