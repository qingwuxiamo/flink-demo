package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 19:32
 */
public class Flink11_TableAPI_EventTime {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取数据流
        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //将流转为动态表，并指定事件时间
//        Table table = tableEnvironment.fromDataStream(waterSensorSingleOutputStreamOperator,
//                $("id"), $("ts"), $("vc"), $("et").rowtime());
        //指定某个字段为事件时间
        Table table = tableEnvironment.fromDataStream(waterSensorSingleOutputStreamOperator,
                $("id"), $("ts").rowtime(), $("vc"));

        table.execute().print();
    }
}
