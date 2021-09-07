package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批次处理（基本不用，因为使用流处理）
 * @author zhoums
 * @version 1.0
 * @date 2021/9/5 10:08
 */
public class Flink01_Batch_WordCount {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文件读取数据
        DataSource<String> textFile = env.readTextFile("input/word.txt");

        //3.转换数据格式
        //a.匿名内部类
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = textFile
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] split = s.split(" ");
                        for (String word : split) {
                            //采集器将数据发送到下游
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                });

        //b.自定义类
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator1 = textFile.flatMap(new MyFlatMap());

        //c.匿名函数
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = textFile
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = line.split(" ");
                    for (String word : split) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                //java为了兼容1.5（之前没有泛型），会对泛型进行泛型擦除，擦除为object，所以匿名函数需要指定返回类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4.按照word进行分组 下标
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = flatMapOperator1.groupBy(0);

        //5.分组内聚合统计 下标
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        //6.打印结果
        sum.print();
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Long>>{

        /**
         *
         * @param s 输入的数据
         * @param collector 采集器
         * @throws Exception
         */
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
            String[] split = s.split(" ");
            for (String word : split) {
                //采集器将数据发送到下游
                collector.collect(Tuple2.of(word, 1L));
            }
        }
    }
}
