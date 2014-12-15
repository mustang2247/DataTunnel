package com.ganqiang.datatunnel.meta.mongodb;

import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.conf.Pool;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.meta.Visitable;
import com.ganqiang.datatunnel.meta.Visitor;
import com.ganqiang.datatunnel.util.StringUtil;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoDBContextHandler implements Visitable {
	
	private static final Logger logger = Logger.getLogger(MongoDBContextHandler.class);

	private Pool pool;
	
	public MongoDBContextHandler(Pool pool){
		this.pool = pool;
	}

	public void init() {
		MongoClient mongoClient = null;
		try {
			String[] str =  pool.getUrl().split("/");
			String url = str[0];
			String database = str[1];
			
			Builder builder = new Builder();
			builder.connectionsPerHost(1000);
			builder.connectTimeout(15000);
			builder.socketTimeout(0);
			builder.maxWaitTime(5000);
			builder.threadsAllowedToBlockForConnectionMultiplier(5000);
			builder.cursorFinalizerEnabled(false);
			builder.connectionsPerHost(pool.getPoolSize());
			MongoClientOptions mco = builder.build();
			
			if (StringUtil.isNullOrBlank(pool.getUserName()) && StringUtil.isNullOrBlank(pool.getPassword())){
				mongoClient = new MongoClient(new ServerAddress(url), new MongoClientOptions.Builder().build());
			} else {
				mongoClient = new MongoClient(new ServerAddress(url),					                       
						Arrays.asList(MongoCredential.createPlainCredential(pool.getUserName(), 
								database, pool.getPassword().toCharArray())), mco);				
			}
			
			DB db = mongoClient.getDB(database);
			Constants.mongodb_map.put(pool.getId(), db);

		} catch (UnknownHostException e) {
			logger.error("mongodb is fault. ", e);
		}
		
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visitMongoDBContext(this);
	}
	
	

}
