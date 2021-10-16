package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 19:32
 */
public class Flink10_TableAPI_ProcessingTime {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //将流转为动态表，并指定处理时间  proctime为格林尼治时间,可以不放在最后
        Table table = tableEnvironment.fromDataStream(streamSource,
                $("id"), $("ts"), $("vc"), $("pt").proctime());

        //不能使用 已经存在的字段The proctime attribute 'ts' must not replace an existing field
//        Table table = tableEnvironment.fromDataStream(streamSource,
//                $("id"), $("ts").proctime(), $("vc"));


        table.execute().print();
    }
}
