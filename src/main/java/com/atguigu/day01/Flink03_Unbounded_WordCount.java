package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/5 11:21
 */
public class Flink03_Unbounded_WordCount {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //2.获取无界流数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop105", 9999);

        //3.先利用flatMap将数据按照空格切分
        SingleOutputStreamOperator<String> wordStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.将上游flatmap发送过来的每一个单词组成Tuple2
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneStream = wordStream.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });

        //5.将相同单词的数据聚和到一块
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //6.累加计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        result.print();

        //开启任务
        env.execute();

    }
}
