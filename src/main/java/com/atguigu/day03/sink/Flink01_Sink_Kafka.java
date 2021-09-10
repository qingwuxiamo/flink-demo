package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 19:06
 */
public class Flink01_Sink_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> map = stream
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        String[] split = value.split(" ");
                        WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                        String jsonString = JSON.toJSONString(waterSensor);
                        return jsonString;
                    }
                });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop105:9092");

        //TODO 将数据发至kafka
        map
                .addSink(new FlinkKafkaProducer<String>(
                        "sensor",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}
