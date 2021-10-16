package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/17 13:46
 */
public class Flink07_FUN_UDF_UDTF {

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
//                .joinLateral(call(MyUDTF.class,$("id")))
//                .select($("id"),$("word"))
//                .execute()
//                .print();

        //注册一个自定义函数
        tableEnv.createTemporaryFunction("MyUDTF", MyUDTF.class);

        table
                .joinLateral(call("MyUdtf", $("id")))
                .select($("id"), $("word"))
                .execute()
                .print();

        //SQL
//        tableEnv.executeSql("select word,id from " + table + " join LATERAL TABLE(MyUdtf(id)) on True").print();
    }

    //自定义一个UDTF函数，将传进来id按照"_"切分
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class MyUDTF extends TableFunction<Row> {
        public void eval(String value) {
            String[] split = value.split("_");
            for (String word : split) {
                collect(Row.of(word));
            }
        }
    }
}
