package com.guohaoyu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
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

public class FlinkCDC_DataStreamAPI_Deser {
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
    /**
     * 封装数据格式
     * {
     *     "database":"gmall_flink-210726",
     *     "tableName":"aaa",
     *     "after":{"id":"123","name":"zs"...},
     *     "before":{"id":"123","name":"zs"...},
     *     "type":"insert",
     * }
     */

    public static class MyDeser implements DebeziumDeserializationSchema<String>{

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //创建一个JSONObject用来存放数据
            JSONObject result = new JSONObject();
            //获取数据库名
            String topic = sourceRecord.topic();
            String[] split = topic.split(".");
            String database = split[1];
            //获取表名
            String tableName = split[2];

            //获取after数据
            Struct value = (Struct) sourceRecord.value();
            Struct after = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            //判断是否有after数据
            if (after!=null) {
                List<Field> fields = after.schema().fields();
                for (Field field : fields) {
                    afterJson.put(field.name(),after.get(field));
                }
            }
            //获取before数据
            Struct before = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            //判断是否有before数据
            if (before!=null) {
                List<Field> fields = before.schema().fields();
                for (Field field : fields) {
                    afterJson.put(field.name(),before.get(field));
                }
            }
            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)){
                type="insert";
            }
            //封住数据
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
