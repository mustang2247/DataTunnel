package com.ganqiang.datatunnel.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.conf.JobConfigHandler;
import com.ganqiang.datatunnel.conf.Pool;
import com.ganqiang.datatunnel.conf.PoolConfigHandler;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.meta.db.DBContextHandler;
import com.ganqiang.datatunnel.meta.hbase.HBaseContextHandler;
import com.ganqiang.datatunnel.meta.hdfs.HdfsContextHandler;
import com.ganqiang.datatunnel.meta.hive.HiveContextHandler;
import com.ganqiang.datatunnel.meta.mongodb.MongoDBContextHandler;
import com.ganqiang.datatunnel.meta.redis.RedisContextHandler;

public class Prepare implements Visitor {
	private static final Logger logger = Logger.getLogger(Prepare.class);

	private static Collection<Visitable> conf_list = new ArrayList<Visitable>();
	private static Collection<Visitable> list = new ArrayList<Visitable>();

	public Prepare() {
		conf_list.add(new PoolConfigHandler());
		conf_list.add(new JobConfigHandler());
	}

	private void setDataPool(){
		for (String key : Constants.conf_pool_map.keySet()) {
			Pool pool = Constants.conf_pool_map.get(key);
			if (!pool.isOpen()) {
				continue;
			}
			if (pool.getType().contains("Mysql")) {
				list.add(new DBContextHandler(pool));
			} else if (pool.getType().contains("Oracle")) {
				list.add(new DBContextHandler(pool));
			} else if(pool.getType().contains("Hive")){
				list.add(new HiveContextHandler(pool));
			} else if(pool.getType().contains("HBase")){
				list.add(new HBaseContextHandler(pool));
			} else if(pool.getType().contains("Hdfs")){
				list.add(new HdfsContextHandler(pool));
			} else if(pool.getType().contains("MongoDB")){
				list.add(new MongoDBContextHandler(pool));
			} else if(pool.getType().contains("Redis")){
				list.add(new RedisContextHandler(pool));
			} 
		}
	}

	@Override
	public void visitAll() {
		Iterator<Visitable> citerator = conf_list.iterator();
		while (citerator.hasNext()) {
			Visitable o = citerator.next();
			o.accept(this);
		}
		setDataPool();
		Iterator<Visitable> iterator = list.iterator();
		while (iterator.hasNext()) {
			Visitable o = iterator.next();
			o.accept(this);
		}
	}

	@Override
	public void visitPoolConfig(PoolConfigHandler handler) {
		logger.info("loading pool configuration...");
		handler.loading();
		logger.info("loading pool configuration finish.");
	}

	@Override
	public void visitJobConfig(JobConfigHandler handler) {
		logger.info("loading job configuration...");
		handler.loading();
		logger.info("loading job configuration finish.");
	}

	@Override
	public void visitDBContext(DBContextHandler handler) {
		logger.info("loading db configuration...");
		handler.init();
		logger.info("loading db configuration finish.");
	}

	@Override
	public void visitHBaseContext(HBaseContextHandler handler) {
		logger.info("loading hbase configuration...");
		handler.init();
		logger.info("loading hbase configuration finish.");
	}
	
	@Override
	public void visitHdfsContext(HdfsContextHandler handler) {
		logger.info("loading hdfs configuration...");
		handler.init();
		logger.info("loading hdfs configuration finish.");
	}

	@Override
	public void visitHiveContext(HiveContextHandler handler) {
		logger.info("loading hive configuration...");
		handler.init();
		logger.info("loading hive configuration finish.");
	}
	
	@Override
	public void visitMongoDBContext(MongoDBContextHandler handler) {
		logger.info("loading mongodb configuration...");
		handler.init();
		logger.info("loading mongodb configuration finish.");
	}

	@Override
	public void visitRedisContext(RedisContextHandler handler) {
		logger.info("loading redis configuration...");
		handler.init();
		logger.info("loading redis configuration finish.");
	}

}
