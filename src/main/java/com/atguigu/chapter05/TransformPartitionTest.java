package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author atguigu-mqx
 */
public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.addSource(new ClickSource())
//                .rebalance()
                .partitionCustom(new Partitioner<String>() {
                    @Override
                    public int partition(String key, int numPartitions) {
                        return (int)(key.charAt(1) - 'a') % 4;
                    }
                }, new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.user;
                    }
                })
                .print();

        env.execute();
    }
}
