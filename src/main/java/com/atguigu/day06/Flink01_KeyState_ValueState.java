package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
 * @author zhoums
 * @version 1.0
 * @date 2021/9/13 11:19
 */
public class Flink01_KeyState_ValueState {

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
                //检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义状态变量
                    private ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        valueState = getRuntimeContext().getState(
                            new ValueStateDescriptor<Integer>("value-state", Types.INT)
                        );
                    }

                    //如果连续的两个水位线差值超过10，就输出报警
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //使用状态
                        if (valueState.value() != null && Math.abs(valueState.value() - value.getVc()) > 10){
                            out.collect("报警！！！相差水位值超过10");
                        }

                        //更新状态
                        valueState.update(value.getVc());
                    }


                })
                .print();

        env.execute();
    }
}
