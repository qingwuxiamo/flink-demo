package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/17 18:02
 */
public class Flink09_FUN_UDF_UDATF {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //不注册直接使用自定义函数
        //f0,f1
//        table
//                .groupBy($("id"))
//                .flatAggregate(call(MyUDATF.class, $("vc")))
//                .select($("id"), $("f0"), $("f1"))
//                .execute()
//                .print();

        //起别名
//        table
//                .groupBy($("id"))
//                .flatAggregate(call(MyUDATF.class, $("vc")).as("value", "rank"))
//                .select($("id"), $("value"), $("rank"))
//                .execute()
//                .print();

        //注册一个自定义函数
        tableEnv.createTemporaryFunction("MyUDATF", MyUDATF.class);

        table
                .groupBy($("id"))
                .flatAggregate(call("MyUDATF", $("vc")).as("value", "rank"))
                .select($("id"), $("value"), $("rank"))
                .execute()
                .print();

    }

    //自定义一个UDATF函数，用来求vc的Top2
    public static class Top2Acc {
        public Integer first;
        public Integer second;
    }

    public static class MyUDATF extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Acc> {

        @Override
        public Top2Acc createAccumulator() {
            Top2Acc top2acc = new Top2Acc();
            top2acc.first = Integer.MIN_VALUE;
            top2acc.second = Integer.MIN_VALUE;
            return top2acc;
        }

        //累加操作
        public void accumulate(Top2Acc top2acc, Integer value) {
            //先比较当前数据是否大于第一
            if (value > top2acc.first) {
                //先将之前的第一名置为第二名
                top2acc.second = top2acc.first;
                //再将当前数据置为第一名
                top2acc.first = value;
            } else if (value > top2acc.second) {
                //不大于第一但是大于第二
                top2acc.second = value;
            }
        }

        //发送结果
        public void emitValue(Top2Acc top2Acc, Collector<Tuple2<Integer, Integer>> out) {

            if (top2Acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(top2Acc.first, 1));
            }
            if (top2Acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(top2Acc.second, 2));
            }
        }
    }
}
