package com.malf.bigdata.gmall.realtime.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class Kafka2Flink2Kafka {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 0. 设置一下状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck100");

        // 1. 开启checkpoint: 参数checkpoint的周期(注入barrier的周期)
        env.enableCheckpointing(5000);
        // 2. 设置checkpoint的一致性语义: 严格一次
        env.getCheckpointConfig().setCheckpointingMode(EXACTLY_ONCE);
        // 3. 设置checkpoint的超时时间: 一分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 4. 设置同时进行的checkpoint的个数为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 5. 设置两个checkpoint之间的最小间隔: 前面结束500ms毫秒之后, 下一个才是
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 6. 当job取消的时候, 是否删除checkpoint数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        Properties sourceProps = new Properties();
        sourceProps.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sourceProps.setProperty("group.id", "Flink10_Kafka_Flink_Kafka4");

        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env
                .addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), sourceProps))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value,
                                        Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);

        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sinkProps.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");

        stream
                .addSink(new FlinkKafkaProducer<Tuple2<String, Long>>(
                        "default",
                        new KafkaSerializationSchema<Tuple2<String, Long>>() {

                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> t,
                                                                            @Nullable Long timestamp) {
                                return new ProducerRecord<>("s2", (t.f0 + "_" + t.f1).getBytes(StandardCharsets.UTF_8));
                            }
                        },
                        sinkProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE  // 开启两阶段提交, 开启事务
                ));

        stream
                .addSink(new SinkFunction<Tuple2<String, Long>>() {
                    @Override
                    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                        if (value.f0.contains("x")) {
                            // 让程序抛出异常, 由于开启了 checkpoint, 所以会自动重启
                            throw new RuntimeException("包含了x, 抛出一个异常....");
                        }
                    }
                });


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
