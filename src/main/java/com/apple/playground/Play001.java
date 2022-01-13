package com.apple.playground;

import akka.remote.artery.aeron.TaskRunner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Play001 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Girl> dataStream = env.fromElements(new Girl("myj", 18, "Jiuquan"), new Girl("lss", 20, "Zhuhai"), new Girl("dxy", 18, "Linyi"));
//        SingleOutputStreamOperator<Girl> ageStream = dataStream.flatMap(new FlatMapFunction<Girl, Girl>() {
//            @Override
//            public void flatMap(Girl value, Collector<Girl> out) throws Exception {
//                value.age++;
//                out.collect(value);
//            }
//        });
        SingleOutputStreamOperator<Girl> ageStream = dataStream.flatMap(new MinusAge(3));

        ageStream.print();

        env.execute();
    }

    protected static class AddAge implements FlatMapFunction<Girl, Girl> {
        private int nums = 10;

        public AddAge() {

        }

        public AddAge(int nums) {
            this.nums = nums;
        }

        @Override
        public void flatMap(Girl value, Collector out) {
            value.age += nums;
            out.collect(value);
        }
    }

    protected static class FilterMYJ implements FlatMapFunction<Girl, Girl> {

        @Override
        public void flatMap(Girl value, Collector out) {
            if (value.name.equals("myj")) {
                out.collect(value);
            }
        }
    }

    protected static class MinusAge extends RichFlatMapFunction<Girl, Girl> {
        private int nums;

        public MinusAge() {

        }

        public MinusAge(int nums) {
            this.nums = nums;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open");
        }

        @Override
        public void flatMap(Girl value, Collector<Girl> out) throws Exception {
            value.age -= nums;
            out.collect(value);
        }

        @Override
        public void close() throws Exception {
            System.out.println("Close");
        }
    }
}
