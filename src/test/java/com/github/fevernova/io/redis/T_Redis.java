package com.github.fevernova.io.redis;


import com.github.fevernova.Common;
import com.github.fevernova.framework.common.Util;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RTopic;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;


public class T_Redis {


    private Redisson redis;


    @Before
    public void init() {

        Config redisConfig = new Config();
        SingleServerConfig singleServerConfig = redisConfig.useSingleServer();
        singleServerConfig.setAddress("redis://127.0.0.1:6379");
        singleServerConfig.setDatabase(0);
        singleServerConfig.setClientName("fevernova");
        this.redis = (Redisson) Redisson.create(redisConfig);
    }


    @Test
    public void T_publish() {

        Common.warmup();
        RTopic topic = this.redis.getTopic("OrderDetail_10");
        long st = Util.nowMS();
        for (int i = 0; i < 10000; i++) {
            topic.publish("" + i);
        }
        long et = Util.nowMS();
        System.out.println(et - st);
    }
}
