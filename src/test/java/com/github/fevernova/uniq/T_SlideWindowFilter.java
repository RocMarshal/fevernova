package com.github.fevernova.uniq;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.service.uniq.SerializationUtils;
import com.github.fevernova.framework.service.uniq.SlideWindowFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
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

        long time = Util.nowMS();
        Assert.assertTrue(this.filter.unique(123, 9876543210L, time));
        Assert.assertFalse(this.filter.unique(123, 9876543210L, time));
        Assert.assertTrue(this.filter.unique(123, 9876543210L, time + 60000));
    }


    private static final long delta = 100000000L;


    @Test
    public void T_snapshot() throws IOException {

        File f = new File("/tmp/fevernova.snapshot");
        if (f.exists()) {
            f.delete();
        }
        long base = 9000000000L;

        long k = 0, j = 0;
        while (k++ < base) {
            j += k;
        }
        System.out.println(j);
        long st = Util.nowMS();
        for (long i = 0L; i < delta; i++) {
            this.filter.unique(1, base + i, 123123);
        }
        long et = Util.nowMS();

        SerializationUtils.saveData("/tmp/fevernova.snapshot", this.filter);
        System.out.println(et - st);
    }


    @Test
    public void T_reload() {

        SerializationUtils.loadData("/tmp/fevernova.snapshot", this.filter);
        Assert.assertEquals(delta, this.filter.count());
    }

}
