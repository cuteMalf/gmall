package com.malf.bigdata.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.app.BaseAppV1;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class BaseLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new BaseLogApp().envInit(2002,2,"BaseLogApp", GmallConfig.TOPIC_ODS_LOG);
    }

    @Override
    protected void handleDataSteam(StreamExecutionEnvironment environment, DataStreamSource<String> dataStream) {
        //1.etl
        SingleOutputStreamOperator<JSONObject> etledStream= etl(dataStream);
        //2. 纠正新老客户
        validateNewOrOldCustomer(etledStream).print();
        //3.分流，侧输出流
        //4.不同的流输出到不同的topic中

        

    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOldCustomer(SingleOutputStreamOperator<JSONObject> etledStream) {
        /**
         * 1.需要知道新老客户标记是如何生成的 ，common里的is_new 0,1
         * 2.前端埋点采集到的数据可靠性无法保证，可能会出现老访客被标记为新访客的问题，因此需要对该标记进行修复
         */
        return etledStream
                .keyBy((KeySelector<JSONObject, String>) value -> value.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<>("firstVisitTimeState", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws IOException {
                        String firstVisitDate = firstVisitDateState.value();
                        JSONObject common = value.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        String today = new SimpleDateFormat("yyyy-MM-dd").format(value.getLong("ts"));

                        if ("1".equals(isNew)){
                            if (firstVisitDate==null){
                                firstVisitDateState.update(today);
                            }else if (!today.equals(firstVisitDate)){
                                common.put("is_new","0");

                            }
                        }else {
                            //老用户，证明一定是老用户
                            //来的是老用户，如果状态为null，把状态设置为前一天的时间
                            if (firstVisitDate ==null){
                                String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(value.getLong("ts") - 24 * 60 * 60 * 1000);
                                firstVisitDateState.update(yesterday);
                            }
                        }
                        return value;
                    }
                });

    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> dataStream) {
        return dataStream.filter((FilterFunction<String>) value -> {
            try {
                JSON.parseObject(value);
            } catch (Exception e) {
                System.out.println("日志格式错误-->  "+value);
                return false;
            }
            return true;
        }).map(JSON::parseObject);
    }
}
