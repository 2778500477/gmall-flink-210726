package com.guohaoyu.util;

import com.guohaoyu.bean.TransientSink;
import com.guohaoyu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql){
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //通过反射的方式获取T对象的所有属性
                        Class<?> clz = t.getClass();
                        Field[] declaredFields = clz.getDeclaredFields();
                        //遍历属性,取出值个预编译sql对象占位符赋值
                        int j = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];
                            //获取字段的注解信息
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink!=null){
                                j++;
                                continue;
                            }
                            //暴力反射
                            field.setAccessible(true);
                            Object value = null;
                            try {
                                value = field.get(t);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                            //给占位符赋值
                            preparedStatement.setObject(i+1-j,value);
                        }
                    }
                },new JdbcExecutionOptions.Builder()
                        .withBatchSize(2)
                        .withBatchIntervalMs(2000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());
    }
}
