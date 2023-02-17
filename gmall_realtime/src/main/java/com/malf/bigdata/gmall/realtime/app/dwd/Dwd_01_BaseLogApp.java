package com.malf.bigdata.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.app.BaseAppV1;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

public class Dwd_01_BaseLogApp extends BaseAppV1 {

    private final String START = "start";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String ERROR = "error";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new Dwd_01_BaseLogApp().envInit(2002, 2, "BaseLogApp", GmallConfig.TOPIC_ODS_LOG);
    }

    @Override
    protected void handleDataStream(StreamExecutionEnvironment environment, DataStreamSource<String> dataStream) {
        //1.etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(dataStream);
        //2. 纠正新老客户
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOldCustomer(etledStream);
        //3.分流，侧输出流
        HashMap<String, DataStream<JSONObject>> fiveStream = splitStream(validatedStream);
//        fiveStream.get(DISPLAY).print(DISPLAY);
        //4.不同的流输出到不同的topic中,写入到kafka中
        write2Kafka(fiveStream);


    }

    private void write2Kafka(HashMap<String, DataStream<JSONObject>> fiveStream) {
        // 分流之后写入到kafka
        fiveStream.get(START).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(GmallConfig.TOPIC_DWD_TRAFFIC_START));
        fiveStream.get(DISPLAY).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(GmallConfig.TOPIC_DWD_TRAFFIC_DISPLAY));
        fiveStream.get(ACTION).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(GmallConfig.TOPIC_DWD_TRAFFIC_ACTION));
        fiveStream.get(ERROR).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(GmallConfig.TOPIC_DWD_TRAFFIC_ERR));
        fiveStream.get(PAGE).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(GmallConfig.TOPIC_DWD_TRAFFIC_PAGE));
    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {
        /**
         * 有五种日志，使用测输出流，需要四个测输出流，一个主流
         * 主流：启动
         * 测输出流：曝光，活动，错误，页面
         * 使用process,使用测输出流
         */
        // 匿名内部类
        OutputTag<JSONObject> outputTagDisplay = new OutputTag<JSONObject>("display") {
        };
        OutputTag<JSONObject> outputTagAction = new OutputTag<JSONObject>("action") {
        };
        OutputTag<JSONObject> outputTagError = new OutputTag<JSONObject>("error") {
        };
        OutputTag<JSONObject> outputTagPage = new OutputTag<JSONObject>("page") {
        };

        // 主流
        SingleOutputStreamOperator<JSONObject> startStream = validatedStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) {
                JSONObject start = value.getJSONObject("start");
                //1.主流 start
                if (start != null) {
                    out.collect(value);
                } else {
                    JSONObject common = value.getJSONObject("common");
                    JSONArray displays = value.getJSONArray("displays");
                    JSONArray actions = value.getJSONArray("actions");
                    JSONObject error = value.getJSONObject("err");

                    //2.侧输出流 display
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //把common 和 ts 放到display中
                            display.putAll(common);
                            display.put("ts", value.getLong("ts"));
                            ctx.output(outputTagDisplay, display);

                        }
                        value.remove("displays"); //把数据中的displays删掉
                    }

                    //3.侧输出流action
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            //把common 和 ts 放到action 中
                            action.putAll(common);
                            ctx.output(outputTagAction, action);

                        }
                        value.remove("actions"); //把数据中的actions删掉
                    }

                    //4.侧输出流err
                    if (error != null) {
                        ctx.output(outputTagError, value);
                        value.remove("err"); //把数据中的err删掉

                    }

                    //5.侧输出流page
                    ctx.output(outputTagPage, value);

                }

            }
        });
        HashMap<String, DataStream<JSONObject>> fiveStreamMap = new HashMap<>();
        fiveStreamMap.put(START, startStream);
        fiveStreamMap.put(DISPLAY, startStream.getSideOutput(outputTagDisplay));
        fiveStreamMap.put(ACTION, startStream.getSideOutput(outputTagAction));
        fiveStreamMap.put(ERROR, startStream.getSideOutput(outputTagError));
        fiveStreamMap.put(PAGE, startStream.getSideOutput(outputTagPage));

        return fiveStreamMap;
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

                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                firstVisitDateState.update(today);
                            } else if (!today.equals(firstVisitDate)) {
                                common.put("is_new", "0");

                            }
                        } else {
                            //老用户，证明一定是老用户
                            //来的是老用户，如果状态为null，把状态设置为前一天的时间
                            if (firstVisitDate == null) {
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
                System.out.println("日志格式错误-->  " + value);
                return false;
            }
            return true;
        }).map(JSON::parseObject);
    }
}
