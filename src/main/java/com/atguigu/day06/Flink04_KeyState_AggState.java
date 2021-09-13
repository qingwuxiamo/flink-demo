package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 计算每个传感器的水位和
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/13 11:19
 */
public class Flink04_KeyState_AggState {

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
                //计算每个传感器的平均水位
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义状态变量
                    private AggregatingState<Integer, Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggregatingState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                        "aggregating-state",
                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                            @Override
                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                return Tuple2.of(0, 0);
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                return accumulator.f0 * 1d / accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                            }
                                        },
                                        Types.TUPLE(Types.INT, Types.INT)
                                )
                        );
                    }

                    //计算每个传感器的水位和
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //使用状态
                        //先将当前vc保存到状态中求平均值
                        aggregatingState.add(value.getVc());

                        //从状态中取出平均值
                        Double avgVc = aggregatingState.get();

                        out.collect(value.getId() + "平均值:" + avgVc);
                    }


                })
                .print();

        env.execute();
    }
}
