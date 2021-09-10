package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 19:06
 */
public class Flink02_Sink_Redis {

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


        //TODO 将数据发至redis
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("hadoop105").build();

        map
                .addSink(new RedisSink<>(config, new RedisMapper<WaterSensor>() {
                    /**
                     * 第一个：redis的操作
                     * 第二个：hash情况下，hash外面的key； 普通情况下，填不填都无效
                     * @return
                     */
                    @Override
                    public RedisCommandDescription getCommandDescription() {
//                        return new RedisCommandDescription(RedisCommand.HSET,"0426");
                        return new RedisCommandDescription(RedisCommand.SET,"TIGA");
                    }

                    /**
                     * hash情况下，指里面的小key
                     * 普通情况下，是大key
                     * @param waterSensor
                     * @return
                     */
                    @Override
                    public String getKeyFromData(WaterSensor waterSensor) {
                        return waterSensor.getId();
                    }

                    /**
                     * 写入的数据
                     * @param waterSensor
                     * @return
                     */
                    @Override
                    public String getValueFromData(WaterSensor waterSensor) {
                        String jsonString = JSON.toJSONString(waterSensor);
                        return jsonString;
                    }
                }));

        env.execute();
    }
}
