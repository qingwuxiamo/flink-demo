package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/7 16:19
 */
public class Flink02_Source_File {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setParallelism(1);

        //读取本地文件
//        env
//                //可以不写文件名，只写到文件夹(如果文件夹下有多个文件，会自动遍历多个文件)
//                .readTextFile("input")
//                .print();

        //可以从hadoop读取文件
        env.readTextFile("hdfs://hadoop105:8020/test1").print();
        env.execute();

    }
}
