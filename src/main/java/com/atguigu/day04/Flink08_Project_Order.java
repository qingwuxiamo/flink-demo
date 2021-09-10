package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 订单支付实时监控
 * 需求: 来自两条流的订单交易匹配
 *
 * @author zhoums
 * @version 1.0
 * @date 2021/9/10 17:54
 */
public class Flink08_Project_Order {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> orderSource = env.readTextFile("input\\OrderLog.csv");

        DataStreamSource<String> receiptSource = env.readTextFile("input\\ReceiptLog.csv");

        //将两条流转为javabean
        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderSource.map(new MapFunction<String, OrderEvent>() {
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
        });

        SingleOutputStreamOperator<TxEvent> txEventStream = receiptSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //连接两个流
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderEventStream.connect(txEventStream);

        connectedStreams
                .keyBy("txId", "txId")
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                    //用于存放orderEvent
                    HashMap<String, OrderEvent> orderMap = new HashMap<>();
                    //用于存放txEvent
                    HashMap<String, TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (txMap.containsKey(value.getTxId())) {
                            //有能够关联上的数据
                            out.collect("订单" + value.getOrderId() + "对账成功");
                            //对账完删除能关联上的数据
                            txMap.remove(value.getTxId());
                        } else {
                            //没有能够关联上的数据 存入缓存
                            orderMap.put(value.getTxId(), value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (orderMap.containsKey(value.getTxId())) {
                            //有能够关联上的数据
                            out.collect("订单" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                            //对账完删除能关联上的数据
                            orderMap.remove(value.getTxId());
                        } else {
                            //没有能够关联上的数据 存入缓存
                            txMap.put(value.getTxId(), value);
                        }
                    }
                })
                .print();

        env.execute();
    }

}
