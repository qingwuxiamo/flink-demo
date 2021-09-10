package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 19:06
 */
public class Flink05_Sink_JDBC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = stream
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                        return waterSensor;
                    }
                });


        //TODO 利用JDBC将数据写入Mysql
        map.addSink(JdbcSink.sink(
                "insert into sensor values (?,?,?)",
                (ps,t)->{
                    ps.setString(1,t.getId());
                    ps.setLong(2,t.getTs());
                    ps.setInt(3,t.getVc());
                },
                JdbcExecutionOptions.builder()
                        //与ES写入数据时一样，通过阈值控制什么时候写入数据，以下设置为来一条写一条
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop105:3306/test?useSSL=false")
                        //指定Driver全类名
                        .withDriverName(Driver.class.getName())
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));
        env.execute();
    }


}
