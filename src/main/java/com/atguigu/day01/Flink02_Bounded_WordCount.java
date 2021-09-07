package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/5 10:25
 */
public class Flink02_Bounded_WordCount {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境（流）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        //2.获取有界流数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.数据按照空格拆分
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(" ");
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //4.分组 将相同单词的数据聚合到一起
        KeyedStream<Tuple2<String, Long>, String> keyBy = flatMap.keyBy(t -> t.f0);

        //5.求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyBy.sum(1);

        //6.打印结果
        sum.print();

        //7.执行
        env.execute();
    }
}
