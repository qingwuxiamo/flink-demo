package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 针对每个传感器输出最高的3个水位值
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/13 11:19
 */
public class Flink02_KeyState_ListState {

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
                //针对每个传感器输出最高的3个水位值
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义状态变量
                    private ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Integer>("list-state", Types.INT)
                        );
                    }

                    //针对每个传感器输出最高的3个水位值
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //使用状态
                        listState.add(value.getVc());
                        //创建list集合用来存放状态中的数据
                        ArrayList<Integer> list = new ArrayList<>();

                        //读取列表状态中的数据，添加到list中
                        for (Integer lastVc : listState.get()) {
                            list.add(lastVc);
                        }
                        //对list集合中的数据升序排序
                        list.sort((t1, t2) -> t2 - t1);

                        //如果list大小大于3，移除第四个数据
                        if (list.size() > 3) {
                            list.remove(3);
                        }

                        //将最大的三条数据存放到列表状态中
                        listState.update(list);
                        out.collect(list.toString());
                    }


                })
                .print();

        env.execute();
    }
}
