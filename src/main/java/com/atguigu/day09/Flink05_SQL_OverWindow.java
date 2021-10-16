package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/17 11:37
 */
public class Flink05_SQL_OverWindow {

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

        //使用Over开窗
//        tableEnv.executeSql("select\n" +
//                "id,\n" +
//                "ts,\n" +
//                "vc,\n" +
//                "sum (vc) over (partition by id order by t)\n" +
//                "from sensor")
//                .print();

        //这种写法可以复用
        tableEnv.executeSql("select " +
                "id," +
                "vc," +
                "count(vc) over w, " +
                "sum(vc) over w " +
                "from sensor " +
                "window w as (partition by id order by t)")
        .print();
    }
}
