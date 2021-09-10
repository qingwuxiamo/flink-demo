package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 11:43
 */
public class Flink04_TransForm_KeyBy {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .socketTextStream("localhost",9999)
                .flatMap(new FlatMapFunction<String, WaterSensor>() {
                    @Override
                    public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                        String[] split = value.split(",");
                        out.collect(new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2])));
                    }
                })
                //分组
                .keyBy(t -> t.getId())
//                .keyBy("id")
//                .keyBy(WaterSensor::getId)
                .print();

        env.execute();
    }
}
