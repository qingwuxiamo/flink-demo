package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 17:47
 */
public class Flink08_SQL_Register_Table {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取数据
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

//        //W1: 1.将流转为表（未注册的表）
//        Table table = tableEnvironment.fromDataStream(waterSensorStream);
//
//        //2.把表注册为一个临时视图
//        tableEnvironment.createTemporaryView("sensor",table);

        //W2: 直接对流进行注册，成为一个临时视图表
        tableEnvironment.createTemporaryView("sensor",waterSensorStream);

//        //3.从临时视图里查询数据，并得到一个新表
//        Table resultTable = tableEnvironment.sqlQuery("select * from sensor where id = 'sensor_1'");
//
//        //4.显示新表数据
//        tableEnvironment.toAppendStream(resultTable, Row.class).print();

        //W3: 直接对表的执行环境调用executeSql()方法，返回的是一个TableResult对象，可以直接打印
        tableEnvironment.executeSql("select * from sensor where id = 'sensor_1'").print();


        env.execute();
    }
}
