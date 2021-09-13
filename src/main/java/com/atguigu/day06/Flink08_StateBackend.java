package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink08_StateBackend {
    public static void main(String[] args) {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 设置状态后端
        //内存
        env.setStateBackend(new MemoryStateBackend());
        //文件系统
        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/checkpoint"));

        //RocksDB
        try {
            env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop105:8020/checkpoint/rockdb"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //barrier不对齐
        env.getCheckpointConfig().enableUnalignedCheckpoints();
    }
}
