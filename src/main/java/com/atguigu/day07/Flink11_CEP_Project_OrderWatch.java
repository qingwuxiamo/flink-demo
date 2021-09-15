package com.atguigu.day07;

import com.atguigu.bean.LoginEvent;
import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/15 15:52
 */
public class Flink11_CEP_Project_OrderWatch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<OrderEvent, Long> orderEventLongKeyedStream = env
                .readTextFile("D:\\Code\\bigdata\\flink-demo\\input\\OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");

                        return new OrderEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                })
                )
                .keyBy(t -> t.getOrderId());

        //定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        //将模式作用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventLongKeyedStream, pattern);

        //获取输出结果
        SingleOutputStreamOperator<String> streamOperator = patternStream
                .select(
                        new OutputTag<String>("timeout") {
                        },
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.toString();
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                                return pattern.toString();
                            }
                        });

        //正常的数据输出到主流
        streamOperator.print();

        //将不正常的数据输出到侧输出流
        streamOperator.getSideOutput(new OutputTag<String>("timeout") {
        }).print("超时数据");
        env.execute();
    }
}
