package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * 统计uv
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 15:03
 */
public class Flink04_Project_UV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input\\UserBehavior.csv");

        streamSource
                //转为javaBean
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new UserBehavior(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4])
                        );
                    }
                })
                //过滤pv行为的数据
                .filter(t -> "pv".equals(t.getBehavior()))
                //将数据组成Tuple2元组
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of("uv", value.getUserId());
                    }
                })
                //对元组 分组
                .keyBy(t -> t.f0)
                //累加 使用set去重
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    //新建一个set集合，用于存放userId
                    HashSet<Long> uids = new HashSet<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        //将userId放入set集合
                        uids.add(value.f1);

                        //返回数据
                        out.collect(Tuple2.of(value.f0, (long) uids.size()));
                    }
                })
                .print();

        env.execute();
    }
}
