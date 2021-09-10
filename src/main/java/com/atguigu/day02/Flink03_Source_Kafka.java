package com.atguigu.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/7 16:30
 */
public class Flink03_Source_Kafka {

    public static void main(String[] args) throws Exception {

        //1. 创建执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setParallelism(1);

        // 0.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop105:9092,hadoop106:9092,hadoop107:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        env.addSource(new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(),properties))
                .print();

        env.execute();


    }
}

