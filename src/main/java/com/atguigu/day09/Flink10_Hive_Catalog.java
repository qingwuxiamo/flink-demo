package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author zhoums
 * @version 1.0
 * @date 2021/9/17 19:02
 */
public class Flink10_Hive_Catalog {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建HiveCatalog

        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "flink_test"; // 默认数据库
        String hiveConfDir     = "C:\\Users\\zhoums\\Desktop\\fsdownload"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

        //创建hiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        //注册HiveCatalog
        tableEnv.registerCatalog(name,hiveCatalog);

        //把 HiveCatalog: myhive 作为当前session的catalog
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        //指定SQL语法为Hive语法
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.sqlQuery("select * from stu").execute().print();

    }
}
