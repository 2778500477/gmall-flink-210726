package com.guohaoyu.func;


import com.alibaba.fastjson.JSONObject;
import com.guohaoyu.common.GmallConfig;
import com.guohaoyu.util.DimUtil;
import com.guohaoyu.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    //声明Phoenix连接
    private Connection connection;
    //声明线程池对象
    private ThreadPoolExecutor threadPoolExecutor;
    //声明属性
    private String tableName;
    public DimAsyncFunction(String tableName){
        this.tableName=tableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        connection= DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor= ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                //查询维度
                try {
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input));
                    //补充信息
                    join(input,dimInfo);
                    //写出数据
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("timeout");
    }


}
