package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/17 13:46
 */
public class Flink06_FUN_UDF_ScalarFun {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);
//        //不注册直接使用自定义函数
//        table
//                .select(call(MyUDF.class, $("id")), $("id"))
//                .execute()
//                .print();

        //注册一个自定义函数
        tableEnv.createTemporaryFunction("MyUDF", MyUDF.class);

//        //TableAPI
//        table
//                .select(call("MyUDF", $("id")), $("id"))
//                .execute()
//                .print();

        //SQL
        tableEnv.executeSql("select MyUdf(id) from " + table).print();
    }

    public static class MyUDF extends ScalarFunction {
        public Integer eval(String value) {
            return value.length();
        }
    }
}
