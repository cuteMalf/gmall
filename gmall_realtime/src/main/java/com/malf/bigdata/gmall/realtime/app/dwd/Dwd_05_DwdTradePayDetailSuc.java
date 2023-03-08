package com.malf.bigdata.gmall.realtime.app.dwd;

import com.malf.bigdata.gmall.realtime.app.BaseSQLApp;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Dwd_05_DwdTradePayDetailSuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_05_DwdTradePayDetailSuc().envInit(2006, 2, "Dwd_05_DwdTradePayDetailSuc");
    }

    @Override
    protected void handleDataTable(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        //1.读取ods_db<>建表
        readOdsDb(tableEnvironment, "Dwd_05_DwdTradePayDetailSuc");
        //2.读取字典表<>建表
        readBaseDic(tableEnvironment);
        //3.ods_db过滤出，支付成功表数据
        Table paymentInfo = tableEnvironment.sqlQuery("select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`pt`, " +
                " ts " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602' ");
        //形成视图
        tableEnvironment.createTemporaryView("payment_info", paymentInfo);
        // 4. 读取下单详情表  (Dwd_03_DwdTradeOrderDetail 写入的数据)
        tableEnvironment.executeSql("create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + FlinkSQlUtil.getKafkaSourceDDL(GmallConfig.TOPIC_DWD_TRADE_ORDER_DETAIL, "Dwd_05_DwdTradePayDetailSuc"));
        // 5. 3 张表进行 join
        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5)); //空闲状态的ttl
        Table result = tableEnvironment.sqlQuery("select " +
                "od.id order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type payment_type_code, " +
                "dic.dic_name payment_type_name, " +
                "pi.callback_time, " +
                "od.source_id, " +
                "od.source_type_code, " +
                "od.source_type_name, " +
                "od.sku_num, " +
                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount split_payment_amount, " +
                "pi.ts, " +
                "od.row_op_ts row_op_ts " +
                "from dwd_trade_order_detail od " +
                "join payment_info pi on od.order_id=pi.order_id " +
                "join base_dic for system_time as of pi.pt as dic on pi.payment_type=dic.dic_code");
        //6.写出到kafka
        tableEnvironment.executeSql("create table dwd_trade_pay_detail_suc( " +
                "order_detail_id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "callback_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_payment_amount string, " +
                "ts bigint, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + FlinkSQlUtil.getKafkaSinkDDL(GmallConfig.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));
        //7.插入到结果表
        result.executeInsert("dwd_trade_pay_detail_suc");


    }
}
