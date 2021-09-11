package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 18:19
 */
public class Flink18_Window_Time_Tumbling_Apply {

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
                //全窗口聚合函数aggly 并不是来一条计算一条，而是等窗口关闭之后统一计算
                .apply(new WindowFunction<Tuple2<String, Integer>, Integer, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {
                        System.out.println("apply.....");
                        for (Tuple2<String, Integer> tuple2 : input) {
                            out.collect(tuple2.f1);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
