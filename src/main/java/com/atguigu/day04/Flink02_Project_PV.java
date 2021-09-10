package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 统计pv
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 11:55
 */
public class Flink02_Project_PV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input\\UserBehavior.csv");

        streamSource
                //转成javaBean
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new UserBehavior(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4]));
                    }
                })
                //过滤含有pv的数据
                .filter(t->("pv").equals(t.getBehavior()))
                //转成tuple2
                .map(new MapFunction<UserBehavior, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {

                        return Tuple2.of(value.getBehavior(),1);
                    }
                })
                //按key分组
                .keyBy(t->t.f0)
                //求和
                .sum(1)
                .print();

        env.execute();
    }
}
