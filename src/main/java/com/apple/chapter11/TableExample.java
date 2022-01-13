package com.apple.chapter11;

import com.apple.chapter05.ClickSource;
import com.apple.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/26  9:18
 */
public class TableExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 基于Table做转换
        Table resultTable1 = eventTable.where($("user").isEqual("Mary"))
                .select($("user"), $("url"));

        // 直接SQL
        Table resultTable2 = tableEnv.sqlQuery("select url, user, `timestamp` from " + eventTable);

        tableEnv.toDataStream(resultTable1)
                .print("1");

        tableEnv.toDataStream(resultTable2)
                .print("2");

        env.execute();

    }
}
