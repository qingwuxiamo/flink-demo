package com.atguigu.day08;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 前面是先得到流, 再转成动态表, 其实动态表也可以直接连接到数据
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 11:43
 */
public class Flink04_TableAPI_Source_Kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //连接kafka获取数据
        Schema schema = new Schema();
        schema
                .field("id", "String")
                .field("ts", "BIGINT")
                .field("vc", "Integer");

        tableEnvironment
                .connect(new Kafka()
                        //kafka通用版本
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .property("group.id", "bigdata")
                        .property("bootstrap.servers", "hadoop105:9092,hadoop106:9092,hadoop107:9092")
                )
                //kafka生产数据格式
//                .withFormat(new Json())
                .withFormat(new Csv())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //获取创建的临时表
        Table sensor = tableEnvironment.from("sensor");

        //对动态表查询数据
        Table resultTable = sensor
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        //动态表转为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnvironment.toRetractStream(resultTable, Row.class);

        retractStream.print();

        env.execute();

    }
}
