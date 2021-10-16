package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 11:23
 */
public class Flink01_TableAPI_Demo {

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

        //3.获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //4.将流转为动态表
        Table table = tableEnvironment.fromDataStream(streamSource);

        //5.连续查询
        Table resultTable = table.
                where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //6.将动态表转为流(只追加流)
        DataStream<Row> resultStream = tableEnvironment.toAppendStream(resultTable, Row.class);

        resultStream.print();

        env.execute();
    }
}
