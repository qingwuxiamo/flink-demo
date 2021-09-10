package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 13:59
 */
public class Flink03_Project_PV_Process {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input\\UserBehavior.csv");

        streamSource.process(new ProcessFunction<String, Tuple2<String,Integer>>() {
            private Integer count = 0;
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
                //按照，切分
                String[] split = value.split(",");

                //转为JavaBean
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );

                //过滤出PV的数据
                if ("pv".equals(userBehavior.getBehavior())) {
                    //在if判断里面做count++
                    count++;
                    out.collect(Tuple2.of(userBehavior.getBehavior(),count));
                }
            }
        }).print();

        env.execute();
    }
}
