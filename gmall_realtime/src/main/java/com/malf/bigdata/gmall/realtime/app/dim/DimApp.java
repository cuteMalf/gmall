package com.malf.bigdata.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.app.BaseAppV1;
import com.malf.bigdata.gmall.realtime.bean.TableProcess;
import com.malf.bigdata.gmall.realtime.common.GmallConstant;
import com.malf.bigdata.gmall.realtime.util.JDBCUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DimApp extends BaseAppV1{
    public static void main(String[] args) {
        new DimApp().envInit(2001,2,"DimApp", GmallConstant.TOPIC_ODS_DB);

    }

    @Override
    protected void handleDataSteam(StreamExecutionEnvironment environment, DataStreamSource<String> dataStream) {
        //1.ods_db读业务数据对数据做清洗，filter
        SingleOutputStreamOperator<JSONObject> odsDbDataStream = filterDirtyData(dataStream);
        //2.flink cdc 读取配置表，并封装为java bean
        SingleOutputStreamOperator<TableProcess> tableProcessStream = readTableProcess(environment);
        //3.根据配置表，在phoenix，进行建表和删表
        createOrDeleteDimTable(tableProcessStream);
        //4.数据流和配置流的，进行connected
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedTwoDataStream = connectTwoStream(odsDbDataStream, tableProcessStream);
        //5.删除维度表中，不需要的字段
        deleteUnwantedCloumns(connectedTwoDataStream);

    }

    private void deleteUnwantedCloumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedTwoDataStream) {
        connectedTwoDataStream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) {
                JSONObject f0 = value.f0;
                TableProcess f1 = value.f1;
                //删除f0中，f1没有的元素
                List<String> stringList = Arrays.asList(f1.getSinkColumns().split(","));
                Set<String> keySet = f0.keySet();
//                Iterator<String> iterator = keySet.iterator();
//                while (!iterator.hasNext()){
//                    String next = iterator.next();
//                    if (!stringList.contains(next)){
//                       iterator.remove();
//                    }
//                }
                keySet.removeIf(key ->!stringList.contains(key)); //上方注释代码的优化



                return Tuple2.of(f0,f1);
            }
        });
    }

    /**
     * 连接odsDb 和 Mysql中配置流
     * @param odsDbDataStream ods_db 数据流
     * @param tableProcessStream mysql 配置流
     * @return connectTwoStream
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectTwoStream(SingleOutputStreamOperator<JSONObject> odsDbDataStream, SingleOutputStreamOperator<TableProcess> tableProcessStream) {
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpStateDesc", String.class, TableProcess.class);
        //1.先把配置流做成广播流
        BroadcastStream<TableProcess> tableProcessBroadcastStream = tableProcessStream.broadcast(tpStateDesc);

        //2.用数据流去connected广播流
        return odsDbDataStream
                .connect(tableProcessBroadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject,TableProcess>>() {
                    @Override
                    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //4.处理数据流中的数据，根据配置信息，从广播状态中读取配置信息
                        //根据key获取对应的配置信息
                        String key = value.getString("table") + ":ALL";
                        ReadOnlyBroadcastState<String, TableProcess> tableProcessBroadcastState = ctx.getBroadcastState(tpStateDesc);
                        TableProcess tableProcess = tableProcessBroadcastState.get(key);

                        //tp 有可能为null,因为有可能是事实表和不需要的维度表
                        if (tableProcess != null) {
                            JSONObject data = value.getJSONObject("data");
                            data.put("opType",value.getString("type"));
                            out.collect(Tuple2.of(data,tableProcess));
                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcess value, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //3.把配置流，写入到广播状态中
                        //3.1获取广播状态
                        BroadcastState<String, TableProcess> tableProcessBroadcastState = ctx.getBroadcastState(tpStateDesc);

                        //3.2写入广播状态
                        String key= value.getSourceTable()+":"+value.getSourceType();
                        tableProcessBroadcastState.put(key,value);

                    }
                });
    }

    private void createOrDeleteDimTable(SingleOutputStreamOperator<TableProcess> tableProcessStream) {
        tableProcessStream
                .filter((FilterFunction<TableProcess>) value -> "dim".equals(value.getSinkType()))
                .process(new ProcessFunction<TableProcess, TableProcess>() {

                    private Connection phoenixConnection;

                    @Override
                    public void open(Configuration parameters) {
                        //open 每个并行度建立一个sql连接,防止频繁连接mysql,给MySQL造成过大的压力
                        //jdbc 连接六步
                        //1.建立连接
                        phoenixConnection = JDBCUtil.getPhoenixConnection();

                    }

                    @Override
                    public void processElement(TableProcess value, ProcessFunction<TableProcess, TableProcess>.Context ctx, Collector<TableProcess> out) throws SQLException {
                        //在这里处理 根据cdc 删表或者建表
                        StringBuilder sql;
                        PreparedStatement preparedStatement;
                        String op = value.getOp();
                        if ("r".equals(op)||"c".equals(op)){
                            sql = getHBaseCreateTableSQL(value);
                            System.out.println("调用了维度建表语句ddl op R= " + sql);
                        } else if ("d".equals(op)){
                            sql = getHBaseDropTableSQL(value);
                            System.out.println("调用了维度删表语句ddl op D= " + sql);
                        }else {
                            //3.获取预处理语句
                            preparedStatement = phoenixConnection.prepareStatement(getHBaseDropTableSQL(value).toString());
                            //4.执行
                            preparedStatement.execute();
                            //5.关闭预处理
                            preparedStatement.close();

                            sql = getHBaseCreateTableSQL(value);
                            System.out.println("调用了维度删表语句ddl op U");
                        }

                        //3.获取预处理语句
                        preparedStatement = phoenixConnection.prepareStatement(sql.toString());
                        //4.执行
                        preparedStatement.execute();
                        //5.关闭预处理
                        preparedStatement.close();

                    }

                    private StringBuilder getHBaseDropTableSQL(TableProcess value) {
                        StringBuilder dropDDL = new StringBuilder("drop table " + value.getSinkTable());
                        return dropDDL;
                    }

                    private StringBuilder getHBaseCreateTableSQL(TableProcess value) {
                        StringBuilder createDDL = new StringBuilder();
                        //2.拼接SQL
                        createDDL
                                .append("create table if not exists ")
                                .append(value.getSinkTable())
                                .append("(")
                                .append(value.getSinkColumns().replaceAll("[^,]+","$0 varchar"))
                                .append(",constraint pk primary key(")
                                .append(value.getSinkPk()==null ||value.getSinkPk()=="" ? "id": value.getSinkPk())
                                .append("))")
                                .append(value.getSinkExtend() == null ? "" : value.getSinkExtend());
                        return createDDL;
                    }

                    @Override
                    public void close() throws SQLException {
                        //6.关闭连接
                        JDBCUtil.close(phoenixConnection);

                    }


                });
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
                .startupOptions(StartupOptions.initial()) //先读取全量数据，然后再根据binlog 捕捉改变的数据, 从状态恢复不会在读取快照
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
                        JSONObject jsonObject = JSON.parseObject(value.replaceAll("bootstrap-", ""));
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");
                        String database = jsonObject.getString("database");
                        return "gmall2022".equals(database) && ("insert".equals(type) || "update".equals(type)) && data != null && data.length() > 2;
                    } catch (Exception e) {
                        System.out.println("JSON 格式非法！");
                        return false;
                    }
                })
                .map((MapFunction<String, JSONObject>) value -> JSON.parseObject(value.replaceAll("bootstrap-", "")));
        return jsonObjectStream;
    }
}
