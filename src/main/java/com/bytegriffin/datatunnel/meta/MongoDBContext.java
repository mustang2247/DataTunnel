package com.bytegriffin.datatunnel.meta;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoDatabase;

public class MongoDBContext implements Initializer {

	private static final Logger logger = LogManager.getLogger(MongoDBContext.class);

	@Override
	public void init(OperatorDefine operator) {
		try {//带用户名/密码的mongodb://user1:pwd1@host1/?authSource=db1&ssl=true
			String address = operator.getAddress();
			MongoClientURI mc = new MongoClientURI(address, getConfigBuilder());
	        @SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient(mc);
	        String databaseName = address.substring(address.lastIndexOf("/") + 1, address.indexOf("?") == -1 ? address.length() :address.indexOf("?"));
	        MongoDatabase database = mongoClient.getDatabase(databaseName);	        
	        Globals.setMongoDatabase(operator.getKey(), database);
			logger.info("任务[{}]加载组件MongoDBContext[{}]的初始化完成。", operator.getName(), operator.getId());
		} catch (RuntimeException re) {
			logger.error("任务[{}]加载组件MongoDBContext[{}]没有连接成功。", operator.getName(), operator.getId(), re);
		}
	}

	private Builder getConfigBuilder() {
		return new MongoClientOptions.Builder().socketKeepAlive(true) // 是否保持长链接
		.connectTimeout(5000) // 链接超时时间
		.socketTimeout(5000) // read数据超时时间
		.readPreference(ReadPreference.primary()) // 最近优先策略
		.connectionsPerHost(30) // 每个地址最大请求数
		.maxWaitTime(1000 * 60 * 2) // 长链接的最大等待时间
		.threadsAllowedToBlockForConnectionMultiplier(50); // 一个socket最大的等待请求数
	}
	
	/**
	 * 根据where条件设置多个查询条件
	 * @param sqlMapper
	 */
	public static Document setQueryFilters(SqlMapper sqlMapper){
		Document searchQuery = new Document();
		if(Strings.isNullOrEmpty(sqlMapper.getWhere()) || sqlMapper.getWhere().replaceAll(" ", "").equals("1=1")){
			return searchQuery;
		}
		List<String> ands = sqlMapper.getAndCondition(sqlMapper.getWhere());
		List<String> ors = sqlMapper.getOrCondition(sqlMapper.getWhere());
		if (ands != null){//用and连接的查询条件
			ands.forEach(con -> setQueryFilter(con, searchQuery));
		} else if(ors != null){//用or连接的查询条件
			ors.forEach(con -> setQueryFilter(con, searchQuery));
		} else {//只存在一个查询条件
			setQueryFilter(sqlMapper.getWhere(), searchQuery);
		}
		return searchQuery;
	}

	/**
	 * 根据where条件设置单个查询条件
	 * @param condition
	 * @param searchQuery
	 */
	private static void setQueryFilter(String condition, Document searchQuery){
		//获取类似 name = zhangsan 单个where条件 ，暂时不支持like查询
		SqlMapper.getWhereFields(condition).forEach(field -> {
			searchQuery.append(field.getFieldName(), field.getFieldValue());
		});
	}

}
