package com.malf.bigdata.gmall.realtime.app.dwd;

import com.malf.bigdata.gmall.realtime.app.BaseSQLApp;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_07_DwdTradeRefundPaySuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_07_DwdTradeRefundPaySuc().envInit(2008, 2, "Dwd_07_DwdTradeRefundPaySuc ");
    }

    @Override
    protected void handleDataTable(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        //1.读取ods_db的数据
        readOdsDb(tableEnvironment, "Dwd_07_DwdTradeRefundPaySuc ");
        //2.读取字典字典表数据
        readBaseDic(tableEnvironment);
        //3.从ods_db过滤出退款成功的表
        Table refundPayment = tableEnvironment.sqlQuery("select " +
                        "data['id'] id, " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['payment_type'] payment_type, " +
                        "data['callback_time'] callback_time, " +
                        "data['total_amount'] total_amount, " +
                        "pt, " +
                        "ts " +
                        "from ods_db " +
                        "where `database`='gmall2022' " +
                        "and `table`='refund_payment' "
                // 由于模拟数据的问题, 没有 0702, 没有 update
                          /*+
                          "and `type`='update' +
                          "and `old`['refund_status'] is not null " +
                          "and `data`['refund_status']='0702' "*/);
        tableEnvironment.createTemporaryView("refund_payment", refundPayment);
        //4.过滤退单表中的退款成功的明细
        Table orderRefundInfo = tableEnvironment.sqlQuery("select " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['refund_num'] refund_num, " +
                        "`old` " +
                        "from ods_db " +
                        "where `database`='gmall2022' " +
                        "and `table`='order_refund_info' "

                // 由于模拟数据的问题, 没有 0705, 没有 update
                                                 /* +
                                                 "and `type`='update' +
                                                  "and `old`['refund_status'] is not null " +
                                                  "and `data`['refund_status']='0705' "*/);
        tableEnvironment.createTemporaryView("order_refund_info", orderRefundInfo);
        //5.过滤订单表中的退款成功的订单数据
        Table orderInfo = tableEnvironment.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] is not null  " +
                "and `data`['order_status']='1006'  ");
        tableEnvironment.createTemporaryView("order_info", orderInfo);
        //6.4张表进行join
        Table result = tableEnvironment.sqlQuery("select " +
                "rp.id, " +
                "oi.user_id, " +
                "rp.order_id, " +
                "rp.sku_id, " +
                "oi.province_id, " +
                "rp.payment_type, " +
                "dic.dic_name payment_type_name, " +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id, " +
                "rp.callback_time, " +
                "ri.refund_num, " +
                "rp.total_amount, " +
                "rp.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from refund_payment rp " +
                "join order_refund_info ri on rp.order_id=ri.order_id and rp.sku_id=ri.sku_id " +
                "join order_info oi on rp.order_id=oi.id " +
                "join base_dic for system_time as of rp.pt as dic on rp.payment_type=dic.dic_code ");
        //7.导出数据到结果表
        tableEnvironment.executeSql("create table dwd_trade_refund_pay_suc( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "date_id string, " +
                "callback_time string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts bigint, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + FlinkSQlUtil.getKafkaSinkDDL(GmallConfig.TOPIC_DWD_TRADE_REFUND_PAY_SUC));
        result.executeInsert("dwd_trade_refund_pay_suc");

    }
}
