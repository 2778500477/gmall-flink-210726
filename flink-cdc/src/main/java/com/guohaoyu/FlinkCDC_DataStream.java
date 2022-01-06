package com.guohaoyu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_DataStream {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);
        //设置ck和savepoint
        //开启ck,每五秒一次
        env.enableCheckpointing(5000L);
        //指定ck的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务关闭时,保留最后一次ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //指定ck的自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
        //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME","guohoayu");
        //创建Flink-Mysql-CDC的source
        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("ghy980903")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.user_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        //使用CDC source从mysql读取数据
        DataStreamSource<String> stringDataStreamSource = env.addSource(build);
        //打印数据
        stringDataStreamSource.print();
        env.execute();
    }

}
