package com.guohaoyu.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.guohaoyu.bean.TableProcess;
import com.guohaoyu.func.DimSinkFunction;
import com.guohaoyu.func.TableProcessFunction;
import com.guohaoyu.ods.Flink_CDCWithCustomerSchema_Ods;
import com.guohaoyu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //开启ck,时间间隔为5s
        env.enableCheckpointing(5000L);
        //设置ck的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务结束后,保留最后一次的ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置ck自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_db"));
        //设置用户名
        System.setProperty("HADOOP_USER_NAME","guohaoyu");
        //消费kafka中ods_base_db数据
        String topic = "ods_base_db";
        String groupId = "ods_db_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //判断是否为json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //过滤空值数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return !"delete".equals(jsonObject.getString("type"));
            }
        });
        //使用flinkCDC读取配置表创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("ghy980903")
                .databaseList("gmall-flink-210726")
                .tableList("gmall-210726-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new Flink_CDCWithCustomerSchema_Ods.MyDeseri())
                .build();

        DataStreamSource<String> flinkCDCDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = flinkCDCDS.broadcast(mapStateDescriptor);

        //连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //处理广播流数据和主流数据分为kafka和hbase流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbaseTag") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //提取两个流中的数据
        DataStream<JSONObject> hbaseDS = kafkaMainDS.getSideOutput(hbaseTag);
        //将两个流的数据分别写出
        filterDS.print("kafka>>>>>");
        hbaseDS.print("hbase>>>>>");

        hbaseDS.addSink(new DimSinkFunction());
        kafkaMainDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"),element.getString("after").getBytes());
            }
        }));

        env.execute();
    }
}
