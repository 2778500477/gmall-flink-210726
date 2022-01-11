package com.guohaoyu.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.guohaoyu.bean.TableProcess;

import com.guohaoyu.common.GmallConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    //声明Phoenix连接
    private Connection connection;

    //侧输出流标记属性
    private OutputTag<JSONObject> outputTag;

    //map状态描述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //处理jsonObject中封装的数据
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取广播流中的状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        //过滤字段
        if (tableProcess!=null){
            filterColumns(value.getJSONObject("after"),tableProcess.getSinkColumns());
            //分流kafka主流和hbase侧输出流
            //将表名或者主题添加到数据中去
            value.put("sinkTable",tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                out.collect(value);
            }else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                ctx.output(outputTag,value);
            }
        }else {
            System.out.println(key+"不存在!");
        }

    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        //切分字段
        String[] split = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(split);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next->!columnList.contains(next.getKey()));
    }

    //处理广播流
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //解析数据为tableProcess对象
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        PreparedStatement preparedStatement = null;
        try {
            //构建建表语句
            StringBuilder createTableSQL = new StringBuilder("create table if not exist ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] split = sinkColumns.split(",");

            for (int i = 0; i < split.length; i++) {
                //取出列名
                String s = split[i];
                //判断当前字段是否为主键
                if (sinkPk.equals(s)) {
                    createTableSQL.append(s).append(" varchar primary key ");
                } else {
                    createTableSQL.append(s).append(" varchar ");
                }
                //判断是否为最后一个字段
                if (i < split.length - 1) {
                    createTableSQL.append(",");
                }

            }
            createTableSQL.append(")").append(sinkTable);
            //打印建表语句
            System.out.println(createTableSQL);

            //执行

            preparedStatement = connection.prepareStatement(createTableSQL.toString());
        } catch (SQLException e) {
            throw new RuntimeException("创建Phoenix表" + sinkTable + "失败!");
        }finally {
            if (preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
