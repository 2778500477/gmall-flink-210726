package com.guohaoyu.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.guohaoyu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;



public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境,设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置ck,间隔5s
        env.enableCheckpointing(5000L);
        //设置ck的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置程序结束后,保留最后一次的ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //设置ck的自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9092/flinkCDC"));
        //设置用户访问名
        System.setProperty("HADOOP_USER_NAME","guohaoyu");
        //读取kafka中的ods_base_log数据
        String topic = "ods_base_log";
        String groupId = "ods_dwd_base_log_app";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //将数据转化成JSONObject
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    //将转化成功的数据写入主流
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        //识别新老用户
        //按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //使用状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> mapDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //设置状态,保存上一次访问数据
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取新老用户标记
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                //判断是否为1
                if ("1".equals(isNew)) {
                    //获取状态信息,并判断是否为null
                    String state = valueState.value();
                    if (state == null) {
                        valueState.update("1");
                    } else {
                        //更新数据isNew
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }
                }
                return jsonObject;
            }
        });

        //分流,利用侧输出流把不同的日志数据分往不同的主题
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        SingleOutputStreamOperator<JSONObject> pageDS = mapDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                //获取启动数据
                String start = value.getString("start");
                if (start != null) {
                    ctx.output(startTag, value);
                } else {
                    //进来的都是页面日志,先写入主流
                    out.collect(value);
                    //获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            //将曝光数据写入侧输出流
                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });
        //提取侧输出流中的数据
        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        //把数据写入kafka主题
        pageDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //执行任务
        env.execute();
    }
}
