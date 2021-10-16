package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 19:50
 */
public class Flink12_SQL_ProcessingTime {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //在建表时指定处理时间
        tableEnvironment.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as PROCTIME()) with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

        tableEnvironment.executeSql("select * from sensor").print();
    }
}
