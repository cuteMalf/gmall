package com.malf.bigdata.gmall.realtime.util;

import com.malf.bigdata.gmall.realtime.common.GmallConfig;

public class FlinkSQlUtil {
    public static String getKafkaSourceDDL(String topic, String groupId) {
        return "with(" +
                " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "', " +
                " 'properties.bootstrap.servers' = '" + GmallConfig.KAFKA_BROKERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'format' = 'json'" +
                ")";
    }

    public static String getKafkaSinkDDL(String topic) {
        return "with(" +
                " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "', " +
                " 'properties.bootstrap.servers' = '" + GmallConfig.KAFKA_BROKERS + "', " +
                " 'format' = 'json'" +
                ")";
    }

    public static String getUpsertKafkaSinkDDL(String topic) {
        return "with(" +
                " 'connector' = 'upsert-kafka', " +
                " 'topic' = '" + topic + "', " +
                " 'properties.bootstrap.servers' = '" + GmallConfig.KAFKA_BROKERS + "', " +
                " 'key.format' = 'json', " +
                " 'value.format' = 'json' " +
                ")";
    }
}
