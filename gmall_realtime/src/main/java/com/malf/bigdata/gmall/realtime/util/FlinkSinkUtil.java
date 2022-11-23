package com.malf.bigdata.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.bean.TableProcess;
import com.malf.bigdata.gmall.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkSinkUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }
}
