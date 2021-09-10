package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 11:56
 */
public class Flink08_TransForm_MaxWithMaxBy {

    public static void main(String[] args) throws Exception {

        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从socket中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        streamSource
                .flatMap(new FlatMapFunction<String, WaterSensor>() {
                    @Override
                    public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                        String[] word = value.split(",");
                        out.collect(new WaterSensor(word[0], Long.parseLong(word[1]), Integer.parseInt(word[2])));
                    }
                })
                //对相同key的数据进行分组并分区
                .keyBy(t -> t.getId())
                //使用聚和算子 Max求水位的最大值
//                .max("vc")
//                .maxBy("vc",true)
                .maxBy("vc",false)
                .print();

        env.execute();
    }
}
