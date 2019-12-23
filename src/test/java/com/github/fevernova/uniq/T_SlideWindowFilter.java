package com.github.fevernova.uniq;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.JobTags;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.service.state.storage.FSStorage;
import com.github.fevernova.framework.service.uniq.SlideWindowFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class T_SlideWindowFilter {


    private static final long delta = 100000000L;

    private SlideWindowFilter filter;

    private FSStorage fsStorage;


    @Before
    public void init() {

        JobTags jobTags = JobTags.builder()
                .jobType("testtype")
                .jobId("testid")
                .cluster("testclr")
                .unit(1)
                .level("l3")
                .deployment("testdpl")
                .podName("testpod")
                .podTotalNum(3)
                .podIndex(0)
                .build();
        GlobalContext globalContext = GlobalContext.builder().jobTags(jobTags).build();
        TaskContext taskContext = new TaskContext("test");

        this.filter = new SlideWindowFilter(globalContext, taskContext);
        this.fsStorage = new FSStorage(globalContext, taskContext);
    }


    @Test
    public void T_base() {

        long time = Util.nowMS();
        Assert.assertTrue(this.filter.unique(123, 9876543210L, time));
        Assert.assertFalse(this.filter.unique(123, 9876543210L, time));
        Assert.assertTrue(this.filter.unique(123, 9876543210L, time + 60000));
    }


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

        this.fsStorage.saveBinary(null, null, this.filter);
        System.out.println(et - st);
    }


    @Test
    public void T_reload() {

        this.fsStorage.recoveryBinary(null, this.filter);
        Assert.assertEquals(delta, this.filter.count());
    }

}
