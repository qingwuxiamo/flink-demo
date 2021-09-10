package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 各省份页面广告点击量实时统计
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 16:49
 */
public class Flink07_Project_Ad {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input\\AdClickLog.csv");

        streamSource
                //转为javabean
                .map(new MapFunction<String, AdsClickLog>() {
                    @Override
                    public AdsClickLog map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new AdsClickLog(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                split[2],
                                split[3],
                                Long.parseLong(split[4])
                        );
                    }
                })
                //转成tuple2元组
                .map(new MapFunction<AdsClickLog, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(AdsClickLog value) throws Exception {
                        return Tuple2.of(value.getProvince(),1);
                    }
                })
                //按省份分组
                .keyBy(t->t.f0)
                //求和
                .sum(1)
                .print();


        env.execute();
    }
}
