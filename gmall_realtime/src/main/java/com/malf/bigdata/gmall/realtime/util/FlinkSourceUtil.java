package com.malf.bigdata.gmall.realtime.util;

import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers(GmallConfig.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return kafkaSource;

    }

}
