package com.malf.bigdata.gmall.realtime.app.dwd;

import com.malf.bigdata.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_04_TradeCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_04_TradeCancelDetail().envInit(2005,2,"Dwd_04_TradeCancelDetail");
    }
    @Override
    protected void handleDataTable(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        //1.读取ods_db,建立输入表
        readOdsDb(tableEnvironment,"Dwd_04_TradeCancelDetail");
        //2.读取字典表数据，建立输入表
        readBaseDic(tableEnvironment);
        //3.筛选数据
        Table table = tableEnvironment.sqlQuery("");
        tableEnvironment.createTemporaryView("",table);


    }
}
