package com.malf.bigdata.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.app.BaseAppV1;
import com.malf.bigdata.gmall.realtime.bean.TableProcess;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Dwd_08_BaseDBApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_08_BaseDBApp().envInit(2009, 2, "Dwd_08_BaseDBApp", GmallConfig.TOPIC_ODS_DB);
    }

    @Override
    protected void handleDataStream(StreamExecutionEnvironment environment, DataStreamSource<String> dataStream) {
        //1.读取ods_bd topic数据，然后etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(dataStream);
        //2.通过cdc读取配置信息
        readTableProcess(environment);
        //3. 数据流和配置流进行 connect


    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment environment) {
        MySqlSource<String> mySqlSourceCdc = MySqlSource
                .<String>builder()
                .hostname("hadoop162")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username("root")
                .password("aaaaaa")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())//先读取全量数据，然后再根据binlog 捕捉改变的数据, 从状态恢复不会在读取快照
                .build();
        return environment
                .fromSource(mySqlSourceCdc, WatermarkStrategy.noWatermarks(), "tableProcess")
                .map(new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String op = jsonObject.getString("op");
                        TableProcess tableProcess;
                        if ("d".equals(op)) {
                            // 如果是删除, 取 before, 封装到 TableProcess 中
                            tableProcess = jsonObject.getObject("before", TableProcess.class);
                        } else {
                            tableProcess = jsonObject.getObject("after", TableProcess.class);
                        }
                        tableProcess.setOp(op);

                        return tableProcess;
                    }
                })
                .filter(value -> value.getSourceType().equals("dwd"));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> dataStream) {
        return dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                try {
                    JSONObject jsonObject = JSON.parseObject(value.replaceAll("bootstrap", ""));
                    String type = jsonObject.getString("type");
                    String data = jsonObject.getString("data");
                    return "gmall2022".equals(jsonObject.getString("database"))
                            && ("insert".equals(type) || "update".equals(type))
                            && data != null
                            && data.length() > 2;
                } catch (Exception e) {
                    System.out.println("你的 json 格式数据异常: " + value);
                    return false;
                }
            }
        }).map((MapFunction<String, JSONObject>) value -> JSON.parseObject(value.replaceAll("bootstrap", "")));
    }
}
