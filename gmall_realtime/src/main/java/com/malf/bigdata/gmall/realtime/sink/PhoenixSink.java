package com.malf.bigdata.gmall.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.malf.bigdata.gmall.realtime.bean.TableProcess;
import com.malf.bigdata.gmall.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    private DruidDataSource druidDataSource;
    @Override
    public void open(Configuration parameters) {
        druidDataSource = DruidDSUtil.getDataSource();
    }

    //流中的数据来一条，这个方法就会执行一次
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws SQLException {
        //1.在连接池获取连结，用完并归还
        DruidPooledConnection phoenixConnection = druidDataSource.getConnection();
        //2.拼接sql
        StringBuilder sql = new StringBuilder();
        String sinkColumns = value.f1.getSinkColumns();
        String placeholder = sinkColumns.replaceAll("[^,]+", "?");

        sql
                .append("upsert into ")
                .append(value.f1.getSinkTable())
                .append("(")
                .append(sinkColumns)
                .append(") values(")
                .append(placeholder)
                .append(")");

        //3.获取预处理语句
        PreparedStatement preparedStatement = phoenixConnection.prepareStatement(sql.toString());
        //3.1给占位符复制
        String[] split = sinkColumns.split(",");
        for (int i = 0; i < split.length; i++) {
            preparedStatement.setString(i+1,value.f0.getString(split[i]));
        }

        System.out.println("调用了phoenix upsert 语句-->"+ sql);

        //4.执行预处理
        preparedStatement.execute();

        //5.手动提交，phoenix没有自动提交
        phoenixConnection.commit();

        //6.关闭预处理语句
        preparedStatement.close();

        //7.归还连接
        phoenixConnection.close(); //从连接池获取的是归还，从DriverManager 获取的是关闭


    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
