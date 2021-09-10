package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 11:53
 */
public class Flink05_TransForm_Shuffle {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                out.collect(value);
            }
        }).setParallelism(2);

        //TODO 对数据进行shuffle
        DataStream<String> shuffle = flatMap.shuffle();


//        KeyedStream<WaterSensor, Tuple> keyedStream = flatMap.keyBy("id");

//        flatMap.keyBy(WaterSensor::getId)
//        flatMap.keyBy(r->r.getId())


        flatMap.print("原始数据").setParallelism(2);
        shuffle.print("shuffle之后的数据").setParallelism(2);

        env.execute();
    }
}
