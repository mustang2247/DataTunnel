package com.ganqiang.datatunnel.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.tomcat.jdbc.pool.DataSource;

import redis.clients.jedis.JedisPool;

import com.ganqiang.datatunnel.conf.Pool;
import com.ganqiang.datatunnel.conf.Task;
import com.ganqiang.datatunnel.meta.hbase.HBaseConnController;
import com.mongodb.DB;

public final class Constants
{

	/******** config ************/
	public static Map<String, Pool> conf_pool_map = new HashMap<String, Pool>();
	public static List<Task> conf_job_list = new ArrayList<Task>();
	public static final String class_type = "class";
	public static final String sql_type =  "sql";
	public static final String hql_type = "hql";

	/******** timer ************/
	public static Timer timer = null;

	/******** chain map,  key:pairid value:chain ************/
	public static Map<String, Chain> chain_map = new HashMap<String, Chain>();

	/******** db datasource key:reader/writer pool id value:reader/writer id value: datasource************/
	public static HashMap<String, DataSource> db_datasource_map = new HashMap<String, DataSource>();
	/******** hive datasource key:reader/writer pool id value:reader/writer id value: datasource************/
	public static HashMap<String, DataSource> hive_datasource_map = new HashMap<String, DataSource>();
	/******** hbase map,  key:poolid value:chain ************/
	public static Map<String, HBaseConnController> hbase_conn_map = new HashMap<String, HBaseConnController>();
	/******** hdfs map,  key:poolid value:filesystem ************/
	public static Map<String, FileSystem> hdfs_map = new HashMap<String, FileSystem>();
	/******** mongodb map,  key:poolid value:db ************/
	public static Map<String, DB> mongodb_map = new HashMap<String, DB>();
	/******** redis map,  key:poolid value:db ************/
	public static Map<String, JedisPool> redis_map = new HashMap<String, JedisPool>();

}
