package com.guohaoyu.common;

public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL0726_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //clickHouse的url地址
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";

    //clickHouse的driver驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
