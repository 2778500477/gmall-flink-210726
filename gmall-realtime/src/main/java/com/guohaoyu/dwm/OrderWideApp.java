package com.guohaoyu.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.guohaoyu.bean.OrderDetail;
import com.guohaoyu.bean.OrderInfo;
import com.guohaoyu.bean.OrderWide;
import com.guohaoyu.func.DimAsyncFunction;
import com.guohaoyu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置ck
        env.enableCheckpointing(5000L);
        //设置ck的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务结束时保留最后一次的ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置ck的自动`重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,3000L));
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/OrderWideApp"));
        //设置用户名
        System.setProperty("HADOOP_USER_NAME","guohaoyu");
        //消费kafka,订单和订单明细的数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_210726";

        DataStreamSource<String> orderInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailKafkaDS  = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));
        //将数据转化为javaBean并提取时间戳生成watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoWatermarkStrategy  = orderInfoKafkaDS.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
            //yyyy-MM-dd HH:mm:ss
            String create_time = orderInfo.getCreate_time();
            String[] dateTime = create_time.split(" ");
            orderInfo.setCreate_date(dateTime[0]);
            orderInfo.setCreate_hour(dateTime[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWatermarkStrategy = orderDetailKafkaDS.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail orderDetail, long l) {
                return orderDetail.getCreate_ts();
            }
        }));

        KeyedStream<OrderInfo, Long> orderInfokeyedStream = orderInfoWatermarkStrategy.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWatermarkStrategy.keyBy(OrderDetail::getOrder_id);
        //双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfokeyedStream.intervalJoin(orderDetailKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
        //关联维度信息
        //关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws Exception {
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long curTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();
                        long age = (curTs - ts) / (1000L * 60 * 60 * 24 * 365);
                        input.setUser_age((int) age);

                        input.setUser_gender(dimInfo.getString("GENDER"));
                    }
                }
                , 100, TimeUnit.SECONDS);

        //关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS  = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws Exception {
                        //提取维度信息并补全字段
                        input.setProvince_name(dimInfo.getString("NAME"));
                        input.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        input.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        input.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                },
                100, TimeUnit.SECONDS);
        //关联sku
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS  = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws Exception {
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 100, TimeUnit.SECONDS);
        //关联spu
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 100, TimeUnit.SECONDS);

        //关联category品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 100, TimeUnit.SECONDS);

        //关联trademark品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 100, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print(">>>>>>>>");
        //将数据写入kafka   dwd_order_wide
        orderWideWithCategory3DS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        //启动任务
        env.execute();
    }
}
