package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/13 11:19
 */
public class Flink05_KeyState_MapState {

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
                //去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义状态变量
                    private MapState<Integer, WaterSensor> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Integer, WaterSensor>(
                                        "map-state",
                                        Integer.class,
                                        WaterSensor.class
                                )
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //使用状态
                        //先去重
                        if (mapState.contains(value.getVc())) {
                            out.collect("此水位已经存在");
                        } else {
                            //将不重复的数据保存道mapState中
                            mapState.put(value.getVc(), value);
                            out.collect(value.toString());
                        }
                    }


                })
                .print();

        env.execute();
    }
}
