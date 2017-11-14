package com.bytegriffin.datatunnel.write;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.sql.*;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.JedisCommands;

import java.util.List;

public class RedisWriter implements Writeable {

    private static final Logger logger = LogManager.getLogger(RedisWriter.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        JedisCommands jedis = Globals.getJedisCommands(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        List<String> sqls = SqlParser.getWriteSql(msg.getRecords(), opt.getValue());
        write(jedis, sqls);
        logger.info("线程[{}]调用RedisWriter执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    private void write(JedisCommands jedis, List<String> sqls) {
        if (sqls == null || sqls.isEmpty()) {
            return;
        }
        try {
            String firstSql = sqls.get(0).toLowerCase().trim();
            if (firstSql.contains("delete")) {//delete操作:支持and/or
                sqls.stream().filter(sql -> !Strings.isNullOrEmpty(sql)).forEach(sql -> {
                    DeleteObject delete = SqlMapper.delete(sql);
                    if (Strings.isNullOrEmpty(delete.getWhere())) {//如果没有where条件,全部删除
                        jedis.del(delete.getWhere());
                    } else {//如果有where条件
                        List<String> ands = delete.getAndCondition(delete.getWhere());
                        List<String> ors = delete.getOrCondition(delete.getWhere());
                        if (ands != null) {//用and连接的查询条件
                            ands.stream().filter(con -> !Strings.isNullOrEmpty(con)).forEach(con -> {
                                SqlMapper.getWhereFields(con).forEach(field -> {
                                    jedis.hdel(delete.getTableName(), field.getFieldName());
                                });
                            });
                        } else if (ors != null) {//用or连接的查询条件
                            ors.stream().filter(con -> !Strings.isNullOrEmpty(con)).forEach(con -> {
                                SqlMapper.getWhereFields(con).forEach(field -> {
                                    jedis.hdel(delete.getTableName(), field.getFieldName());
                                    return;
                                });
                            });
                        } else {//只存在一个查询条件
                            SqlMapper.getWhereFields(delete.getWhere()).forEach(field -> {
                                jedis.hdel(delete.getTableName(), field.getFieldName());
                            });
                        }
                    }
                });
            } else if (firstSql.contains("insert")) {
                sqls.stream().filter(sql -> !Strings.isNullOrEmpty(sql)).forEach(sql -> {
                    InsertObject insert = SqlMapper.insert(sql);
                    insert.getFields().stream().filter(field -> field != null).forEach(field -> {
                        jedis.hset(insert.getTableName(), field.getFieldName(), field.getFieldValue().toString());
                    });
                });
            } else if (firstSql.contains("update")) {//update操作：暂不支持where条件
                sqls.stream().filter(sql -> !Strings.isNullOrEmpty(sql)).forEach(sql -> {
                    UpdateObject update = SqlMapper.update(sql);
                    update.getFields().stream().filter(field -> field != null).forEach(field -> {
                        jedis.hset(update.getTableName(), field.getFieldName(), field.getFieldValue().toString());
                    });
                });
            }
        } catch (Exception e) {
            logger.error("不能执行更新sql: [{}]", sqls, e);
        }
    }

}
