package com.atguigu.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 18:19
 */
public class Flink16_Window_Time_Tumbling_Aggre_Func {

    public static void main(String[] args) throws Exception {

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        streamSource
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(t -> t.f0)
                //开启一个基于时间的滚动窗口 大小为5秒
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //增量聚合函数aggregate （可以改变数据的类型）
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                    //初始化累加器
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("初始化累加器");
                        return 0;
                    }

                    //累加器的累加操作
                    @Override
                    public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                        System.out.println("累加操作");
                        return value.f1+accumulator;
                    }

                    //获取累加结果
                    @Override
                    public Integer getResult(Integer accumulator) {
                        System.out.println("获取结果");
                        return accumulator;
                    }

                    //合并累加器 （此方法只在会话窗口中使用）
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        System.out.println("Merge...");
                        return null;
                    }
                })
                .print();


        env.execute();
    }
}
