package com.bytegriffin.datatunnel.read;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.SelectObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.bytegriffin.datatunnel.write.RedisWriter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import redis.clients.jedis.JedisCommands;

public class RedisReader implements Readable{

	private static final Logger logger = LogManager.getLogger(RedisWriter.class);

	@Override
	public void channelRead(HandlerContext ctx, Param msg) {
		JedisCommands jedisCommands = Globals.getJedisCommands(this.hashCode());
		OperatorDefine opt = Globals.operators.get(this.hashCode());
		String newsql = SqlParser.getReadSql(opt.getValue());
		List<Record> results = select(jedisCommands, newsql);
		msg.setResults(results);
		ctx.write(msg);
		logger.info("线程[{}]调用RedisReader执行任务[{}]",Thread.currentThread().getName(), opt.getKey());
	}

	private List<Record> select(JedisCommands jedisCommands, String sql) {
		if(Strings.isNullOrEmpty(sql)){
			return null;
		}
		if (Strings.isNullOrEmpty(sql)) {
			return null;
		}
		SelectObject select = SqlMapper.select(sql);
		List<Record> list = Lists.newArrayList();
		String tableName = select.getTableName();
		List<String> columnList = select.getColumn();
		
		return list;
	}

}
