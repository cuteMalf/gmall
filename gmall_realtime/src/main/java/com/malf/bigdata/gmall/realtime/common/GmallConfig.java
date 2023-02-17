package com.malf.bigdata.gmall.realtime.common;

public class GmallConfig {

    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop162:9092,hadoop164:9092";
    //ods_db 是maxwell监听binlog,产生的cdc数据
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_ODS_LOG ="ods_log";

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop162:3306/gmall_config?useSSL=false&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";

    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";
    public static final String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";

    public static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop162:8123/gmall2022";
    public static final String REDIS_HOST = "hadoop162";
    public static final int REDIS_PORT = 6379;

    public static final int TWO_DAY = 2 * 24 * 60 * 60;




}
