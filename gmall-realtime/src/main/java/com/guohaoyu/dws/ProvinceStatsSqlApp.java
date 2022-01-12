package com.guohaoyu.dws;

import com.guohaoyu.bean.ProvinceStats;
import com.guohaoyu.util.ClickHouseUtil;
import com.guohaoyu.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        /*
        //检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","guohaoyu");
        */
        //使用ddl的方式创建动态表,提取事件时间生成WaterMark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("create table order_wide( "+
                "   province_id bigint, "+
                "   province_name String, "+
                "   province_area_code String, "+
                "   province_iso_code String, "+
                "   province_3166_2_code String, "+
                "   order_id bigint, "+
                "   split_total_amount decimal, "+
                "   create_time String, "+
                "   rt as TO_TIMESTAMP(create_time), "+
                "   watermark for rt as rt - interval '2' second "+
                ")with ("+ MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId)+") ");
        //计算订单数  订单总金额  开窗  10秒的滚动窗口
        Table resultTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code," +
                "province_iso_code,province_3166_2_code," +
                "COUNT( DISTINCT order_id) order_count, sum(total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts "+
                " from ORDER_WIDE group by TUMBLE(rt, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        //将动态表转化成流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(resultTable, ProvinceStats.class);
        //将数据写出
        provinceStatsDataStream.addSink(ClickHouseUtil.
                <ProvinceStats>getJdbcSink("insert into  province_stats_210526  values(?,?,?,?,?,?,?,?,?,?)"));

        //启动任务
        env.execute();
    }
}
