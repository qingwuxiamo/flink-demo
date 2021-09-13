package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink07_OperateState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> localStream = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> hadoopStream = env.socketTextStream("hadoop102", 9999);

        //3.定义广播状态
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("state", Types.STRING, Types.STRING);

        BroadcastStream<String> broadcast = localStream.broadcast(mapStateDescriptor);

        //connect连接两个流
        BroadcastConnectedStream<String, String> connect = hadoopStream.connect(broadcast);

        connect.process(new BroadcastProcessFunction<String, String, String>() {

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                //提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //将数据放入广播状态中
                broadcastState.put("switch", value);
            }

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //获取状态中的值
                String s = broadcastState.get("switch");

                if ("1".equals(s)) {
                    out.collect("执行逻辑1。。。");
                } else if ("2".equals(s)) {
                    out.collect("执行逻辑2。。。");
                } else {
                    out.collect("执行其他逻辑");
                }
            }

        });




        env.execute();

    }
}
