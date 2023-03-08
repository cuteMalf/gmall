package com.malf.bigdata.gmall.realtime.app.dwd;

import com.malf.bigdata.gmall.realtime.app.BaseSQLApp;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Dwd_06_DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_06_DwdTradeOrderRefund().envInit(2007, 2, "Dwd_06_DwdTradeOrderRefund");
    }

    @Override
    protected void handleDataTable(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        //1.读取ods_db
        readOdsDb(tableEnvironment, "Dwd_06_DwdTradeOrderRefund");
        //2.读取字典表
        readBaseDic(tableEnvironment);
        //3.从ods_db过滤订单表
        Table orderRefundInfo = tableEnvironment.sqlQuery("select \" +\n" +
                "                                                  \"data['id'] id, \" +\n" +
                "                                                  \"data['user_id'] user_id, \" +\n" +
                "                                                  \"data['order_id'] order_id, \" +\n" +
                "                                                  \"data['sku_id'] sku_id, \" +\n" +
                "                                                  \"data['refund_type'] refund_type, \" +\n" +
                "                                                  \"data['refund_num'] refund_num, \" +\n" +
                "                                                  \"data['refund_amount'] refund_amount, \" +\n" +
                "                                                  \"data['refund_reason_type'] refund_reason_type, \" +\n" +
                "                                                  \"data['refund_reason_txt'] refund_reason_txt, \" +\n" +
                "                                                  \"data['create_time'] create_time, \" +\n" +
                "                                                  \"pt, \" +\n" +
                "                                                  \"ts \" +\n" +
                "                                                  \"from ods_db \" +\n" +
                "                                                  \"where `database`='gmall2022'  \" +\n" +
                "                                                  \"and `table`='order_refund_info'  \" +\n" +
                "                                                  \"and `type`='insert'  ");
        //4.形成视图
        tableEnvironment.createTemporaryView("order_refund_info", orderRefundInfo);
        //5.order_refund_info
        Table orderInfo = tableEnvironment.sqlQuery("select " +
                "data['id'] id," +
                "data['province_id'] province_id," +
                "`old` " +
                "from ods_db " +
                "where `database`='gmall2022'  " +
                "and `table`='order_info'  " +
                "and `type`='update'  " +
                "and `old`['order_status'] is not null " +
                "and `data`['order_status']='1005' )");
        //6.形成视图
        tableEnvironment.createTemporaryView("order_info", orderInfo);
        //7.join
        //这是状态的ttl
        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
        Table result = tableEnvironment.sqlQuery("select " +
                "ri.id, " +
                "ri.user_id, " +
                "ri.order_id, " +
                "ri.sku_id, " +
                "oi.province_id, " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
                "ri.create_time, " +
                "ri.refund_type, " +
                "dic1.dic_name, " +
                "ri.refund_reason_type, " +
                "dic2.dic_name, " +
                "ri.refund_reason_txt, " +
                "ri.refund_num, " +
                "ri.refund_amount, " +
                "ri.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_refund_info ri " +
                "join order_info oi on ri.order_id=oi.id " +
                "join base_dic for system_time as of ri.pt as dic1 " +
                "on ri.refund_type=dic1.dic_code " +
                "join base_dic for system_time as of ri.pt as dic2 " +
                "on ri.refund_reason_type=dic2.dic_code ");
        //8.写入到kafka中
        tableEnvironment.executeSql("create table dwd_trade_order_refund(" +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "date_id string, " +
                "create_time string, " +
                "refund_type_code string, " +
                "refund_type_name string, " +
                "refund_reason_type_code string, " +
                "refund_reason_type_name string, " +
                "refund_reason_txt string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts bigint, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + FlinkSQlUtil.getKafkaSinkDDL(GmallConfig.TOPIC_DWD_TRADE_ORDER_REFUND));
//        result.executeInsert("dwd_trade_order_refund");
        tableEnvironment.executeSql("insert into dwd_trade_order_refund select * from " + result);

    }
}
