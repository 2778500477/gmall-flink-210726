package com.guohaoyu.dws;

import com.guohaoyu.bean.KeywordStats;
import com.guohaoyu.common.GmallConstant;
import com.guohaoyu.func.SplitFunction;
import com.guohaoyu.util.ClickHouseUtil;
import com.guohaoyu.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
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
        //DDL建表,提取事件时间生成watermark
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";

        tableEnv.executeSql("create table page_log( "+
                "   page map<string,string>, "+
                "   ts bigint, "+
                "   rt AS TO_TIMESTAMP_LTZ(ts,3),"+
                "   WATERMARK FOR rt AS rt - INTERVAL '2' SECOND "+
                ")with ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId) +")");
        //过滤数据  只需要搜索的数据
        Table filterTable = tableEnv.sqlQuery("select " +
                "   page['item'] keywords, " +
                "   rt " +
                "   from page_log " +
                "   where page['last_page_id'] = 'search' " +
                "   and page['item'] is not null ");
        //注册UDTF并使用其完成切词
        tableEnv.createTemporaryView("filterTable",filterTable);
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        Table splitTable = tableEnv.sqlQuery("select " +
                "   word, " +
                "   rt " +
                "from filter_table,LATERAL TABLE(SplitFunction(keywords)) ");
        tableEnv.createTemporaryView("split_table",splitTable);
        //计算每个分词出现的次数
        Table keywordStatsSearch  = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   "+splitTable
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");


        //将动态表转化成流
        DataStream<KeywordStats> keywordStatsSearchDataStream =
                tableEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);

        //写入ck
        keywordStatsSearchDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats(keyword,ct,source,stt,edt,ts)  " +
                                " values(?,?,?,?,?,?)"));


                //执行
        env.execute();
    }
}
