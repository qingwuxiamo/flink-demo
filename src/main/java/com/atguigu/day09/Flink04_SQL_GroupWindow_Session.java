package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/17 11:37
 */
public class Flink04_SQL_GroupWindow_Session {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")");

        //开启滑动窗口
        tableEnv.executeSql("select \n" +
                " id, \n" +
                " sum(vc) vcSum, \n" +
                " SESSION_START(t,INTERVAL '2' second) as startWindow, \n" +
                " SESSION_END(t,INTERVAL '2' second) as endWindow \n" +
                "from sensor \n" +
                "group by SESSION(t,INTERVAL '2' second), id")
                .print();
    }
}
