package com.malf.bigdata.gmall.realtime.app;

import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.util.FlinkSQlUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public abstract class BaseSQLApp {
    public void envInit(int restPort, int parallelism, String ck) {

        //设置代理用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", restPort);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.setParallelism(parallelism);
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //开启checkpoint,在企业中一般是分钟级别的1，3，5，10
        environment.enableCheckpointing(3000L);
        //设置状态后端：1.memory 2.fs 3.rocksdb
        //1.13之前，本地和checkpoint 都是由状态后端管理 memory （本地：taskManager内存 checkpoint：在jobManager的内存）,hdfs(本地：taskManager的内存 checkpoint：hdfs),rocksDb(本地：rocksdb checkpoint：在hdf上)
        //1.13之后，本地和checkpoint 分开，状态后端只负责本地的存贮，checkpoint有专门的负责。只有memory 和rocksdb。memory-> taskManger中使用HashMapStateEnd存贮.2.rocksdb
        //1.13 checkpoint 有专门的api 负责在管理,1.在jobManager内存 2.在hdfs
        environment.setStateBackend(new HashMapStateBackend());
        //开启checkpoint后，当程序失败时，会从checkpoint中拉去状态进行重启,设置重启的策略，企业中一般是无限重启
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)));//一天之内最多重启10次，每次重启间隔3分钟。

        //设置checkpoint相关的参数
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        //设置checkpoint 的路径
        checkpointConfig.setCheckpointStorage("hdfs://hadoop162:8020/gmall/checkpoint/" + ck);
        //设置checkpoint的模式，至少一次和精确一次
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint最大的并发度,表示最多做几个checkpoint
        //checkpointConfig.setMaxConcurrentCheckpoints(1);
        //设置两次checkpoint 之间的间隔时间。与将并发度设置为1，功能类似，区别是有间隔时间.这两个只需要设置一个即可
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);//两个checkpoint 间隔500ms
        //设置checkpoint的超时时间
        checkpointConfig.setCheckpointTimeout(60 * 1000L);//一分钟的超时时间
        //checkpoint 最大失败次数，在生产环境可能会设置，无限重试，但是如果是代码错误或者数据错误，则不一定能自动修复。可修复某一时段资源不够的问题
        checkpointConfig.setTolerableCheckpointFailureNumber(5);//表示最多失败五次
        //手动取消任务时，是否删除checkpoint
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);//里面传入一个枚举值，一共有三个枚举值

        //创建flink table 运行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        handleDataTable(environment, tableEnvironment);


    }

    protected abstract void handleDataTable(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment);

    public void readOdsDb(StreamTableEnvironment tEnv, String groupId) {
        //建立输入表
        tEnv.executeSql("create table ods_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`ts` bigint, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>," +
                " `pt` as proctime() " +
                ")" + FlinkSQlUtil.getKafkaSourceDDL(GmallConfig.TOPIC_ODS_DB, groupId));

    }

    public void readBaseDic(StreamTableEnvironment tEnv) {
        //建立输入表
        tEnv.executeSql("create table base_dic ( " +
                "  dic_code string, " +
                "  dic_name string " +
                ") WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false', " +
                "  'table-name' = 'base_dic', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa',  " +
                "  'lookup.cache.max-rows' = '10'," +
                "  'lookup.cache.ttl' = '1 hour' " +
                ")");
    }
}
