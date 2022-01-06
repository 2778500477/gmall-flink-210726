package com.guohaoyu.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.guohaoyu.util.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class Flink_CDCWithCustomerSchema_Ods {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置状态后端,间隔周期为5s
        env.enableCheckpointing(5000L);

        //设置ck的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置任务关闭时,保留最后一次ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //指定ck的自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));

        //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME","guohaoyu");

        //创建Flink-Mysql-CDC的source
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("ghy980903")
                .databaseList("gmall-flink-210726")
                .tableList("gmall-flink.user_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeseri())
                .build();

        //使用CDC Source 从MySQL读取数据
        DataStreamSource<String> streamSource = env.addSource(mysqlSource);

        //将数据发送到kafka
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute();
    }
    public static class MyDeseri implements DebeziumDeserializationSchema<String>{

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //创建个JSONObject用来存放结果数据
            JSONObject result = new JSONObject();

            //获取数据库名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];
            //获取表名
            String tableName = split[2];
            //获取after数据
            Struct value = (Struct) sourceRecord.value();
            Struct after = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            //判断after是否有数据
            if (after!=null) {
                List<Field> fields = after.schema().fields();
                for (Field field : fields) {
                    afterJson.put(field.name(),after.get(field));
                }
            }
            //获取before数据
            Struct before = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            if (before!=null) {
                List<Field> fields = before.schema().fields();
                for (Field field : fields) {
                    beforeJson.put(field.name(),before.get(field));
                }
            }

            //获取类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)){
                type="insert";
            }

            //封装数据
            result.put("database",database);
            result.put("tableName",tableName);
            result.put("after",afterJson);
            result.put("before",beforeJson);
            result.put("type",type);

            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
