package com.guohaoyu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.guohaoyu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {
    //从Phoenix表中查询维度数据
    public static JSONObject getDimInfo(Connection connection,String tableName,String pk) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
        //查询redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + pk;
        String dimInfoStr = jedis.get(redisKey);
        //判断redis是否有该数据
        if (dimInfoStr!=null){
            //重置过期时间
            jedis.expire(redisKey,3600*24);
            //归还连接
            jedis.close();
            //返回redis中查询到的数据
            return JSON.parseObject(dimInfoStr);
        }
        //构建sql语句 select * from db.tn where id='1001';
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + pk + "'";
        //查询phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySQL, JSONObject.class, false);

        //将数据写入redis
        JSONObject dimInfo = queryList.get(0);
        jedis.set(redisKey,dimInfo.toJSONString());
        jedis.expire(redisKey,3600*24);
        jedis.close();

        return dimInfo;
    }

    public static void delDimInfo(String tableName,String pk){
        String redisKey = "DIM:"+tableName+":"+pk;
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }
}
