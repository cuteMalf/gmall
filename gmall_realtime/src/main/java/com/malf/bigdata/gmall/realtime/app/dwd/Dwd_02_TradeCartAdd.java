package com.malf.bigdata.gmall.realtime.app.dwd;

import com.malf.bigdata.gmall.realtime.app.BaseSQLApp;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_02_TradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_02_TradeCartAdd().envInit(2003, 2, "Dwd_02_TradeCartAdd");

    }

    @Override
    protected void handleDataTable(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        //1.消费ods_db
        readOdsDb(tableEnvironment, "Dwd_02_TradeCartAdd");
        //2.从topic ods_db中过滤出加购数据
        Table cartInfo = tableEnvironment.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['sku_id'] sku_id, " +
                        " `data`['source_id'] source_id, " +
                        " `data`['source_type'] source_type," +
                        " if(`type`='insert', " +
                        "   `data`['sku_num']," +
                        "    cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)" +
                        " ) sku_num, " +
                        " ts," +
                        " pt " +
                        "from ods_db " +
                        "where `database`='gmall2022' " +
                        "and `table`='cart_info' " +
                        "and (" +
                        " `type`='insert' " +
                        "  or " +
                        "   (`type`='update' " +
                        "     and `old`['sku_num'] is not null " +
                        "     and cast(`data`['sku_num'] as int) > cast(`old`['sku_num']  as int)" +
                        "   )" +
                        ")");
        //3.创建临时视图
        tableEnvironment.createTemporaryView("cart_info", cartInfo);
        //4.读取维度数据，从base_dic 中
        readBaseDic(tableEnvironment);
        //5.lookup join
        Table result = tableEnvironment.sqlQuery("select " +
                "ci.id,  " +
                "user_id,  " +
                "sku_id,  " +
                "source_id,  " +
                "source_type,  " +
                "dic_name source_type_name,  " +
                "sku_num,  " +
                "ts  " +
                "from cart_info ci " +
                "join base_dic for system_time as of ci.pt as dic " +
                "on ci.source_type=dic.dic_code");
        //6.写出到kafka中,相当于建立结果表
        tableEnvironment.executeSql("create table dwd_trade_cart_add(" +
                "id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "source_id string,  " +
                "source_type_code string,  " +
                "source_type_name string,  " +
                "sku_num string,  " +
                "ts bigint  " +
                ")" + FlinkSQlUtil.getKafkaSinkDDL(GmallConfig.TOPIC_DWD_TRADE_CART_ADD));
        result.executeInsert("dwd_trade_cart_add");


    }
}
