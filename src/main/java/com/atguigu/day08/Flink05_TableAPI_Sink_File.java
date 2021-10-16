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
import org.apache.flink.table.descriptors.FileSystem;
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
public class Flink05_TableAPI_Sink_File {

    public static void main(String[] args) throws Exception {

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据流
        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //3.获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Table table = tableEnvironment.fromDataStream(streamSource);

        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //4.连接外部文件系统读
        Schema schema = new Schema();
        schema
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnvironment.connect(
                //可以自动创建
                new FileSystem().path("output/sensor-sql.txt"))
                //指定格式
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //TODO 4.将结果表的数据插入到映射到文件系统的临时表中
        resultTable.executeInsert("sensor");

//        env.execute()方法会去分析代码，生成一些 graph，但是我们代码中没有调用算子，所以会报错，可以直接不用
//        env.execute();

    }
}
