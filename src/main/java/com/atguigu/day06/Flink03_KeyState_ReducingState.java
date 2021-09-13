package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 计算每个传感器的水位和
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/13 11:19
 */
public class Flink03_KeyState_ReducingState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        streamSource
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return WaterSensor.of(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                })
                //将相同id的数据聚和到一块
                .keyBy(t -> t.getId())
                //计算每个传感器的水位和
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义状态变量
                    private ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化reducingState
                        reducingState = getRuntimeContext().getReducingState(
                                new ReducingStateDescriptor<Integer>(
                                        "reducing-state",
                                        new ReduceFunction<Integer>() {
                                            @Override
                                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                                return value1 + value2;
                                            }
                                        },
                                        Types.INT
                                )
                        );
                    }

                    //计算每个传感器的水位和
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //使用状态
                        //将当前的数据存到状态中累加计算
                        reducingState.add(value.getVc());
                        //取出累加后的结果
                        Integer sum = reducingState.get();
                        out.collect(value.getId() + "_" + sum);
                    }


                })
                .print();

        env.execute();
    }
}
