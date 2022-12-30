package com.malf.bigdata.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.bean.TableProcess;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSinkUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }

    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", GmallConfig.KAFKA_BROKERS);
        // 生产的事务超时时间不能超过服务器最大上限(15 分钟)
        properties.setProperty("transaction.timeout.ms", 12 * 60 * 1000 + "");

        return new FlinkKafkaProducer<String>("default", new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
            }
        }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
