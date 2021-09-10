package com.atguigu.day03.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 8:57
 */
public class Flink01_Source_Custom {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new MySource()).print();

        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor>{
        private Random random = new Random();
        private volatile Boolean isRunning = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){
                ctx.collect(new WaterSensor("s"+random.nextInt(100),System.currentTimeMillis(),
                        random.nextInt(100)));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
