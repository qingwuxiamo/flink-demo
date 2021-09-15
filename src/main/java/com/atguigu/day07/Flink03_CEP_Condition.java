package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/15 11:21
 */
public class Flink03_CEP_Condition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = env
                .readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return WaterSensor.of(
                                split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000;
                                    }
                                })
                );

        //TODO 3.定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                //迭代条件   最普遍的条件类型
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                //简单条件  where 相当于 and
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() > 10;
                    }
                })
                //停止条件  防止循环多次带来的内存吃不消问题
                .oneOrMore()
                .until(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return value.getVc() > 30;
                    }
                });
        //组合条件 把多个条件结合起来使用
//                .or(new SimpleCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor value) throws Exception {
//                        return value.getTs() > 3;
//                    }
//                });

        //TODO 4.将模式作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(waterSensorSingleOutputStreamOperator, pattern);

        //TODO 5.获取符合要求的数据
        patternStream
                .select(new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        return map.toString();
                    }
                })
                .print();

        env.execute();
    }
}