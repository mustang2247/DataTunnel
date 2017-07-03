package com.bytegriffin.datatunnel.core;

import java.util.Map;

import javax.sql.DataSource;

import org.apache.hadoop.hbase.client.Connection;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoDatabase;

/**
 * 全局变量：缓存供全局访问的变量
 */
public final class Globals {

	// 关系型数据库缓存 key: task_name_md5_address  value:关系型数据源
	private static Map<String, DataSource> datasource_caches = Maps.newHashMap();
	// 存放task key值，key：hashcode  value：readerdefine/writerdefine
	public static Map<Integer, OperatorDefine> operators = Maps.newHashMap();
	// HBase数据库缓存 key: task_name_md5_address  value：hbase数据库连接
	private static Map<String, Connection> hbase_connection_caches = Maps.newHashMap();
	// Mongo数据库缓存 key: task_name_md5_address  value：hbase数据库连接
	private static Map<String, MongoDatabase> mongo_database_caches = Maps.newHashMap();

	public static void setDataSource(String key, DataSource dataSource){
		datasource_caches.put(key, dataSource);
	}
	
	public static void setHBaseConnection(String key, Connection connection){
		hbase_connection_caches.put(key, connection);
	}

	public static void setMongoDatabase(String key, MongoDatabase database){
		mongo_database_caches.put(key, database);
	}

	public static DataSource getDataSource(Integer hashCode){
		if(operators.containsKey(hashCode)){
			OperatorDefine optdefine = operators.get(hashCode);
			return datasource_caches.get(optdefine.getKey());
		}
		return null;
	}
	
	public static Connection getHBaseConnection(Integer hashCode){
		if(operators.containsKey(hashCode)){
			OperatorDefine optdefine = operators.get(hashCode);
			return hbase_connection_caches.get(optdefine.getKey());
		}
		return null;
	}
	
	public static MongoDatabase getMongoDatabase(Integer hashCode){
		if(operators.containsKey(hashCode)){
			OperatorDefine optdefine = operators.get(hashCode);
			return mongo_database_caches.get(optdefine.getKey());
		}
		return null;
	}

	public static String getKey(Integer hashCode){
		if(operators.containsKey(hashCode)){
			return operators.get(hashCode).getKey();
		}
		return null;
	}

}
