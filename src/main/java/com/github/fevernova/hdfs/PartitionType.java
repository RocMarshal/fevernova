package com.github.fevernova.hdfs;


import org.apache.commons.lang3.Validate;

import java.text.SimpleDateFormat;
import java.util.Date;


public enum PartitionType {

    DAY {
        @Override
        public PartitionRender newInstance(int period) {

            return new DayPartitionRender(period);
        }
    }, HOUR {
        @Override
        public PartitionRender newInstance(int period) {

            return new DayHourPartitionRender(period);
        }
    }, MINUTE {
        @Override public PartitionRender newInstance(int period) {

            return new DayHourMinutePartitionRender(period);
        }
    };


    public abstract PartitionRender newInstance(int period);


    public abstract class PartitionRender {


        protected int period;


        public abstract void render(StringBuilder base);

    }


    public class DayPartitionRender extends PartitionRender {


        private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("/yyyy/MM/dd/");


        public DayPartitionRender(int period) {

            super.period = period;
        }


        @Override
        public void render(StringBuilder base) {

            base.append(simpleDateFormat.format(new Date()));
        }
    }


    public class DayHourPartitionRender extends PartitionRender {


        private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("/yyyy/MM/dd/HH/");


        public DayHourPartitionRender(int period) {

            Validate.isTrue(Constants.HOUR_PERIOD_SET.contains(period));
            super.period = period;
        }


        @Override
        public void render(StringBuilder base) {

            long t = (System.currentTimeMillis() / (period * 3600000) * (period * 3600000));
            base.append(simpleDateFormat.format(new Date(t)));
        }
    }


    public class DayHourMinutePartitionRender extends PartitionRender {


        private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("/yyyy/MM/dd/HH/mm/");


        public DayHourMinutePartitionRender(int period) {

            Validate.isTrue(Constants.MINUTE_PERIOD_SET.contains(period));
            super.period = period;
        }


        @Override
        public void render(StringBuilder base) {

            long t = (System.currentTimeMillis() / (period * 60000) * (period * 60000));
            base.append(simpleDateFormat.format(new Date(t)));
        }
    }

}
