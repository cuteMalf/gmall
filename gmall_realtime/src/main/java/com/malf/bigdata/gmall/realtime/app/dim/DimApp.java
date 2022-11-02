package com.malf.bigdata.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.app.BaseAppV1;
import com.malf.bigdata.gmall.realtime.bean.TableProcess;
import com.malf.bigdata.gmall.realtime.common.GmallConstant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp extends BaseAppV1{
    public static void main(String[] args) {
        new DimApp().envInit(2001,2,"DimApp", GmallConstant.TOPIC_ODS_DB);

    }

    @Override
    protected void handleDataSteam(StreamExecutionEnvironment environment, DataStreamSource<String> dataStream) {
        //1.对数据做清洗，filter
        SingleOutputStreamOperator<JSONObject> JsonDataStream = filterDirtyData(dataStream);
        //2.flink cdc 读取配置表，并封装为java bean
        SingleOutputStreamOperator<TableProcess> tableProcessStream = readTableProcess(environment);
        //3.根据配置表，在phoenix，进行建表和删表

        //4.数据流和配置流的，进行connected

    }
    //通过flink cdc 读取mysql 数据
    //默认一启动，先读取全量数据，然后再根据binlog 捕捉改变的数据
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment environment) {
        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname("hadoop162")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username("root")
                .password("aaaaaa")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial()) //先读取全量数据，然后再根据binlog 捕捉改变的数据
                .build();
        return environment
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "gmall_config.table_process")
                .map((MapFunction<String, TableProcess>) value -> {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcess tableProcess = null;
                    //如果是删除操作,取before
                    if ("d".equals(op)) {
                        tableProcess = jsonObject.getObject("before", TableProcess.class);
                    } else {
                        tableProcess = jsonObject.getObject("after", TableProcess.class);
                    }
                    tableProcess.setOp(op);
                    return tableProcess;
                });

    }

    private SingleOutputStreamOperator<JSONObject> filterDirtyData(DataStreamSource<String> dataStream) {
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = dataStream.filter((FilterFunction<String>) value -> {
                    try {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");
                        String database = jsonObject.getString("database");
                        return "gmall2022".equals(database) && ("insert".equals(type) || "update".equals(type)) && data != null && data.length() > 2;
                    } catch (Exception e) {
                        System.out.println("JSON 格式非法！");
                        return false;
                    }
                })
                .map(JSON::parseObject);
        return jsonObjectStream;

    }
}
