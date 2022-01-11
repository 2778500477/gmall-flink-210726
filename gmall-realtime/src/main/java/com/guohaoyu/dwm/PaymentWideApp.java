package com.guohaoyu.dwm;

import com.alibaba.fastjson.JSON;
import com.guohaoyu.bean.OrderWide;
import com.guohaoyu.bean.PaymentInfo;
import com.guohaoyu.bean.PaymentWide;
import com.guohaoyu.common.GmallConfig;
import com.guohaoyu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启ck
        env.enableCheckpointing(5000L);
        //设置ck的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务完成后保留最后一次的ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置ck的自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/PaymentWideApp"));
        //设置用户名
        System.setProperty("HADOOP_USER_NAME","guohaoyu");
        //读取kafka中的数据 dwd_payment_info  dwm_order_wide
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        //将数据转化成javaBean,并提取WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS.map(line -> {
            PaymentInfo paymentInfo = JSON.parseObject(line, PaymentInfo.class);
            return paymentInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            long parse = sdf.parse(paymentInfo.getCreate_time()).getTime();
                            return parse;
                        } catch (ParseException e) {
                            e.printStackTrace();
                            throw new RuntimeException("时间格式错误!");
                        }

                    }
                }));
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(orderWide.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误!");
                                }
                            }
                        }));
        //分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream = paymentInfoDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream = orderWideDS.keyBy(OrderWide::getOrder_id);
        //双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream)
                .between(Time.seconds(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });
        //将数据写入kafka  dwd_payment_wide
        paymentWideDS.print(">>>>>>>>>>");
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));
        //启动任务
        env.execute();
    }
}
