package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * reduce是sum，min等的底层
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 18:42
 */
public class Flink09_TransForm_Reduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        streamSource
                .flatMap(new FlatMapFunction<String, WaterSensor>() {
                    @Override
                    public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                        String[] split = value.split(",");
                        out.collect(
                                new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]))
                        );
                    }
                })
                .keyBy(t->t.getId())
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("reduce>>>>>>");
                        return new WaterSensor(value1.getId(),value1.getTs(),value1.getVc()+value2.getVc());
                    }
                })
                .print();

        env.execute();
    }
}
