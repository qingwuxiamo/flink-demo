package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 18:42
 */
public class Flink10_TransForm_Process {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        streamSource
                //TODO keyBy之前用processFunction 实现flatmap类似的功能
                .process(new ProcessFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split(" ");
                        out.collect(Tuple2.of(split[0],1));
                    }
                })
                .keyBy(t->t.f0)
               //TODO keyBy之后使用KeyedProcessFunction 实现count类似的功能
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    //定义累加变量，用于保存累加后的结果
                    //有问题，因为无法按照key分辨
                    //---------》需要使用状态变量 valueState
                    private Integer count = 0;
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        count += 1;
                        out.collect(Tuple2.of(value.f0,count));
                    }
                })
                .print();

        env.execute();
    }
}
