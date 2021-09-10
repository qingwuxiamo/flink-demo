package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/7 16:12
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1000L, 40),
                new WaterSensor("ws_002", 2000L, 45),
                new WaterSensor("ws_003", 3000L, 50)
        );

//        env
        //fromCollection
//                .fromCollection(waterSensors)
//                .print();

        //fromElements
        env.fromElements(1, 2, 3, 4)
                .print();

        env.execute();
    }
}
