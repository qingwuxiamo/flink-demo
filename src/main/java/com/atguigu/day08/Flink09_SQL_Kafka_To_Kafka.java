package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 18:28
 */
public class Flink09_SQL_Kafka_To_Kafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建从kafka消费数据的表
        tableEnv.executeSql(("create table source_sensor (id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'sensor',"
                + "'properties.bootstrap.servers' = 'hadoop105:9092,hadoop106:9092,hadoop107:9092',"
                + "'properties.group.id' = 'atguigu',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'csv'"
                + ")"
        ));

        //3.创建写数据到kafka的表
        tableEnv.executeSql(("create table sink_sensor(id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'sensor',"
                + "'properties.bootstrap.servers' = 'hadoop105:9092,hadoop106:9092,hadoop107:9092',"
                + "'format' = 'csv'"
                + ")"
        ));


        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");

    }
}
