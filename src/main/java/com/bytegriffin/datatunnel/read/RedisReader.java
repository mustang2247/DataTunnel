package com.bytegriffin.datatunnel.read;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.SelectObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import redis.clients.jedis.JedisCommands;

public class RedisReader implements Readable{

	private static final Logger logger = LogManager.getLogger(RedisReader.class);

	@Override
	public void channelRead(HandlerContext ctx, Param msg) {
		JedisCommands jedis = Globals.getJedisCommands(this.hashCode());
		OperatorDefine opt = Globals.operators.get(this.hashCode());
		String newsql = SqlParser.getReadSql(opt.getValue());
		List<Record> results = select(jedis, newsql);
		msg.setRecords(results);
		ctx.write(msg);
		logger.info("线程[{}]调用RedisReader执行任务[{}]",Thread.currentThread().getName(), opt.getKey());
	}

	/**
	 * 暂时保留list模式，只支持hash模式，因为hash模式中明确指明了FieldName
	 * @param jedis
	 * @param sql
	 * @return
	 */
	private List<Record> select(JedisCommands jedis, String sql) {
		if(Strings.isNullOrEmpty(sql)){
			return null;
		}
		SelectObject select = SqlMapper.select(sql);
		return hash(jedis, select);
	}
	
	/**
	 * 格式：select fieldname1,fieldname2 from key 不支持where条件
	 * 注意：在hash模式下，一个tableName（即对应一个key）对应拥有多个字段的单条记录
	 * @param jedis
	 * @param select
	 * @return
	 */
	private List<Record> hash(JedisCommands jedis,SelectObject select){
		List<Record> list = Lists.newArrayList();
		String tableName = select.getTableName();
		Record record = new Record();
		List<Field> fields = Lists.newArrayList();
		select.getColumn().forEach(column -> {
			fields.add(new Field(column, jedis.hget(tableName, column)));
		});
		record.setFields(fields);
		list.add(record);
		return list;
	}

	/**
	 * list模式，获取列表，支持select * from aaa where start,end
	 * tableName就是list名称，
	 * @param jedis
	 * @param select
	 * @return
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private List<Record> list(JedisCommands jedis,SelectObject select){
		List<Record> list = Lists.newArrayList();
		String tableName = select.getTableName();
		String where = select.getWhere();
		int start = 0;
		int end = -1;
		if(!Strings.isNullOrEmpty(where)){
			List<String> whereList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(select.getWhere());
			start = Integer.valueOf(whereList.get(0));
			end = Integer.valueOf(whereList.get(1));
		}
		List<String> range = jedis.lrange(tableName, start, end);
		end = end == -1 ? range.size() : end;
		for(int i=start; i<end;i++){//fieldname设置为index值
			list.add(new Record(Lists.newArrayList(new Field(i+"", range.get(i)))));
		}
		return list;
	}

}
