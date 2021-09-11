package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/11 11:51
 */
public class Flink06_Window_EventTime_Tumbling_CustomerEvent {

    public static void main(String[] args) throws Exception {

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将端口读过来数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.分配waterMark，并指定eventTime
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        //分配waterMark类型   自定义周期性waterMark
                        .forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                return new MyPeriodWaterMark(Duration.ofSeconds(2));
                            }
                        })
                        //分配时间戳（事件时间）
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );

        waterSensorSingleOutputStreamOperator
                //5.将相同id的数据聚和到一块
                .keyBy(t->t.getId())
                //6.开启一个基于事件时间的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String msg = "当前key: " + key
                                + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);
                    }
                })
                .print();

        env.execute();
    }

    //TODO 自定义周期性生成WaterMark
    public static class MyPeriodWaterMark implements WatermarkGenerator<WaterSensor>{

        /** The maximum timestamp encountered so far. */
        private long maxTimestamp;

        /** The maximum out-of-orderness that this watermark generator assumes. */
        private final long outOfOrdernessMillis;

        /**
         * Creates a new watermark generator with the given out-of-orderness bound.
         *
         * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
         */
        public MyPeriodWaterMark(Duration maxOutOfOrderness) {

            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

            // start so that our lowest watermark would be Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("生成WaterMark:"+(Math.max(maxTimestamp, eventTimestamp)- outOfOrdernessMillis - 1));
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
