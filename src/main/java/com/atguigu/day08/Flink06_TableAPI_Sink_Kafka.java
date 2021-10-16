package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 前面是先得到流, 再转成动态表, 其实动态表也可以直接连接到数据
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/16 11:43
 */
public class Flink06_TableAPI_Sink_Kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //流转为动态表
        Table table = tableEnvironment.fromDataStream(streamSource);

        //连续查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //TODO 创建输出表 连接Kafka写入数据
        Schema schema = new Schema();
        schema
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnvironment
                .connect(
                        new Kafka()
                                .version("universal")
                                .topic("sensor")
                                .sinkPartitionerRoundRobin()
                                .property("bootstrap.servers", "hadoop105:9092,hadoop106:9092,hadoop107:9092")
                )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //把数据写到输出表中
        resultTable.executeInsert("sensor");

    }
}
