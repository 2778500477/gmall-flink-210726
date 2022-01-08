package com.guohaoyu.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.guohaoyu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //设置ck,5s
        env.enableCheckpointing(5000L);
        //设置ck的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务结束时,保留最后一次的ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置ck的自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,3000L));
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-UniqueVisitApp"));
        //设置用户名访问
        System.setProperty("HADOOP_USER_NAME","guohaoyu");
        //读取kafka中dwd_page_log数据创建流
        String groupId = "unique_visit_app_210726";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        //将数据转换成JOSN对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //使用状态编程的方式去重数据
        SingleOutputStreamOperator<JSONObject> filterDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> mapStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);

                //设置状态的ttl
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                mapStateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getIterationRuntimeContext().getState(mapStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //获取上一跳页面id
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                //判断上一跳是否为null

                if (lastPageId == null) {
                    //获取状态数据
                    String visitDate = valueState.value();
                    String curDate = sdf.format(jsonObject.getLong("ts"));

                    if (visitDate == null || !visitDate.equals(curDate)) {
                        valueState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }

            }
        });
        //将数据写入kafka
        filterDs.print();
        filterDs.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        //执行
        env.execute();
    }
}
