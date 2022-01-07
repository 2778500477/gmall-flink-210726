package com.guohaoyu.func;


import com.alibaba.fastjson.JSONObject;
import com.guohaoyu.common.GmallConfig;
import com.guohaoyu.util.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    //声明phoenix连接
    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection= DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        /*connection.setAutoCommit(true);*/
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //拼接插入数据的sql语句
            String upsertSQL = getUpsertSQL(value.getString("sinkTable"), value.getJSONObject("after"));

            //预编译sql
            preparedStatement = connection.prepareStatement(upsertSQL);

            //如果维度数据更新则先删除redis中的数据
            DimUtil.delDimInfo(value.getString("sinktable").toLowerCase(),value.getJSONObject("after").getString("id"));


            //执行写入操作
     /*   //数据量大可以按批次处理
        preparedStatement.addBatch();
        connection.commit();*/
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入数据失败");
        } finally {
            if (preparedStatement!=null){
                preparedStatement.close();
            }
        }

    }

    private String getUpsertSQL(String sinkTable, JSONObject data) {
        //获取列名和数据
        Set<String> colums = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into"
                +GmallConfig.HBASE_SCHEMA+"."
                +sinkTable
                +"("
                + StringUtils.join(colums,",")
                +") values ('"
                +StringUtils.join(values,"','")
                +"')";
    }
}
