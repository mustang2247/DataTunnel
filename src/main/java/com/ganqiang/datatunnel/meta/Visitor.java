package com.ganqiang.datatunnel.meta;

import com.ganqiang.datatunnel.conf.JobConfigHandler;
import com.ganqiang.datatunnel.conf.PoolConfigHandler;
import com.ganqiang.datatunnel.meta.db.DBContextHandler;
import com.ganqiang.datatunnel.meta.hbase.HBaseContextHandler;
import com.ganqiang.datatunnel.meta.hdfs.HdfsContextHandler;
import com.ganqiang.datatunnel.meta.hive.HiveContextHandler;
import com.ganqiang.datatunnel.meta.mongodb.MongoDBContextHandler;
import com.ganqiang.datatunnel.meta.redis.RedisContextHandler;

public interface Visitor {

	void visitPoolConfig(PoolConfigHandler handler);
	void visitJobConfig(JobConfigHandler handler);
	void visitDBContext(DBContextHandler handler);
	void visitHBaseContext(HBaseContextHandler handler);
	void visitHdfsContext(HdfsContextHandler handler);
	void visitHiveContext(HiveContextHandler handler);
	void visitMongoDBContext(MongoDBContextHandler handler);	
	void visitRedisContext(RedisContextHandler handler);	
	void visitAll();

}
