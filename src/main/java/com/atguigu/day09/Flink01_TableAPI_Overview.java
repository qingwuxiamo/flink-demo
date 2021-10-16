package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/17 11:21
 */
public class Flink01_TableAPI_Overview {

    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据流
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

        //TODO 3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorSingleOutputStreamOperator, $("id"), $("ts").rowtime(), $("vc"));

        //使用over开窗
        table
                //over
//                .window(Over.partitionBy($("id")).orderBy($("ts")).as("w"))
//                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).as("w"))
                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(2L)).as("w"))
                .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")).as("vcSum"))
                .execute()
                .print();

        env.execute();
    }
}
