package com.guohaoyu.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static <T>List<T> queryList(Connection connection,String querySql,Class<T> clz,boolean underScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        //创建集合用来存放查询结果
        ArrayList<T> result = new ArrayList<>();
        //编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //获取列名信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        //遍历resultSet,将每行查询到的数据封装为T对象
        while (resultSet.next()){
            //构建T对象
            T t = clz.newInstance();
            //遍历获取列名数据
            for (int i = 1; i < columnCount+1; i++) {
                String catalogName = metaData.getCatalogName(i);
                Object value = resultSet.getObject(catalogName);
                if (underScoreToCamel){
                    catalogName= CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,catalogName.toLowerCase());
                }
                //给T对象进行属性赋值
                BeanUtils.setProperty(t,catalogName,value);
            }
            //将T对象进行属性赋值
            result.add(t);

        }
        //关闭资源
        resultSet.close();
        preparedStatement.close();

        return result;
    }
}
