package com.ganqiang.datatunnel.meta.redis;


import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.ganqiang.datatunnel.conf.Pool;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.meta.Visitable;
import com.ganqiang.datatunnel.meta.Visitor;

public class RedisContextHandler implements Visitable {

	private Pool pool;

	public RedisContextHandler(Pool pool) {
		this.pool = pool;
	}

	public void init() {
		String[] url = pool.getUrl().split(":");
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(5);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		try {
			JedisPool jedispool = new JedisPool(config, url[0], Integer.valueOf(url[1]));
			Constants.redis_map.put(pool.getId(), jedispool);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visitRedisContext(this);
	}
}
