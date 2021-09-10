package com.atguigu.day03.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/8 19:01
 */
public class Flink11_TransForm_Repartition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .shuffle()
                .print("shuffle").setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .rebalance()
                .print("rebalance").setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .rescale()
                .print("rescale").setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .broadcast()
                .print("broadcast").setParallelism(2);

        env.execute();
    }
}
