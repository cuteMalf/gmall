package com.malf.bigdata.gmall.realtime.app.dwd;

import com.malf.bigdata.gmall.realtime.app.BaseSQLApp;
import com.malf.bigdata.gmall.realtime.common.GmallConfig;
import com.malf.bigdata.gmall.realtime.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_03_TradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_03_TradeOrderDetail().envInit(2004, 2, "Dwd_03_DwdTradeOrderDetail");
    }

    @Override
    protected void handleDataTable(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        //1.读取ods_db,等于输入表
        readOdsDb(tableEnvironment, "Dwd_03_DwdTradeOrderDetail");
        //2.读取字典表，等于输入表
        readBaseDic(tableEnvironment);
        //3.筛选订单明细数据
        Table orderDetail = tableEnvironment.sqlQuery("select " +
                "data['id'] id,  " +
                "data['order_id'] order_id,  " +
                "data['sku_id'] sku_id,  " +
                "data['sku_name'] sku_name,  " +
                "data['create_time'] create_time,  " +
                "data['source_id'] source_id,  " +
                "data['source_type'] source_type,  " +
                "data['sku_num'] sku_num,  " +
                "cast( cast(data['sku_num'] as decimal(16,2)) * " +
                "      cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,  " +
                "data['split_total_amount'] split_total_amount,  " +
                "data['split_activity_amount'] split_activity_amount,  " +
                "data['split_coupon_amount'] split_coupon_amount,  " +
                "ts,  " +
                "pt  " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='order_detail' " +
                "and `type`='insert' ");
        //创建视图
        tableEnvironment.createTemporaryView("order_detail", orderDetail);
        //4.筛选订单表
        Table orderInfo = tableEnvironment.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='order_info' " +
                "and `type`='insert' ");
        //创建视图
        tableEnvironment.createTemporaryView("order_info", orderInfo);
        //5.筛选详情活动表
        Table orderDetailActivity = tableEnvironment.sqlQuery("select " +
                "data['order_detail_id'] order_detail_id," +
                "data['activity_id'] activity_id," +
                "data['activity_rule_id'] activity_rule_id " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert' ");
        //创建视图
        tableEnvironment.createTemporaryView("order_detail_activity", orderDetailActivity);
        //6.筛选详情优惠卷表
        Table orderDetailCoupon = tableEnvironment.sqlQuery("select  " +
                "data['order_detail_id'] order_detail_id,  " +
                "data['coupon_id'] coupon_id  " +
                "from `ods_db`  " +
                "where `database`='gmall2022' " +
                "and `table` = 'order_detail_coupon'  " +
                "and `type` = 'insert' ");
        //创建视图
        tableEnvironment.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        //7. 5张表join
        Table result = tableEnvironment.sqlQuery("select " +
                "od.id,  " +
                "od.order_id,  " +
                "oi.user_id,  " +
                "od.sku_id,  " +
                "od.sku_name,  " +
                "oi.province_id,  " +
                "act.activity_id,  " +
                "act.activity_rule_id,  " +
                "cou.coupon_id,  " +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,  " +
                "od.create_time,  " +
                "od.source_id,  " +
                "od.source_type,  " +
                "dic.dic_name source_type_name,  " +
                "od.sku_num,  " +
                "od.split_original_amount,  " +
                "od.split_activity_amount,  " +
                "od.split_coupon_amount,  " +
                "od.split_total_amount,  " +
                "od.ts, " +
                "current_row_timestamp() row_op_ts  " + /*得到每行计算的时间: 到后面 dws 去重的时候使用*/
                "from order_detail od " +
                "join order_info oi on od.order_id=oi.id " +
                "left join order_detail_activity act on od.id=act.order_detail_id " +
                "left join order_detail_coupon cou on od.id=cou.order_detail_id " +
                "join base_dic for system_time as of od.pt as dic on od.source_type=dic.dic_code ");
        //8.写入到kafka,建立输出表
        tableEnvironment.executeSql("create table dwd_trade_order_detail(  " +
                "id string,  " +
                "order_id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "sku_name string,  " +
                "province_id string,  " +
                "activity_id string,  " +
                "activity_rule_id string,  " +
                "coupon_id string,  " +
                "date_id string,  " +
                "create_time string,  " +
                "source_id string,  " +
                "source_type string,  " +
                "source_type_name string,  " +
                "sku_num string,  " +
                "split_original_amount string,  " +
                "split_activity_amount string,  " +
                "split_coupon_amount string,  " +
                "split_total_amount string,  " +
                "ts bigint,  " +
                "row_op_ts timestamp_ltz(3),  " +
                "primary key(id) not enforced  " +
                ")" + FlinkSQlUtil.getUpsertKafkaSinkDDL(GmallConfig.TOPIC_DWD_TRADE_ORDER_DETAIL));
        //9.插入kafka
        result.executeInsert("dwd_trade_order_detail");

    }
}
