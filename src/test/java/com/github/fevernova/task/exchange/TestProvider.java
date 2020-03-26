package com.github.fevernova.task.exchange;


import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.OrderMatchFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;


@Slf4j
public class TestProvider implements DataProvider<Integer, OrderMatch> {


    protected OrderMatch orderMatch = (OrderMatch) new OrderMatchFactory().createData();

    private boolean flag = false;

    @Getter
    private int count = 0;

    @Setter
    private boolean print;


    public TestProvider(boolean print) {

        this.print = print;
    }


    @Override public OrderMatch feedOne(Integer key) {

        Validate.isTrue(!flag);
        flag = true;
        return orderMatch;
    }


    @Override public void push() {

        Validate.isTrue(flag);
        flag = false;
        count++;
        if (print) {
            log.info(orderMatch.toString());
        }
        orderMatch.clearData();
    }

}
