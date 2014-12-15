package com.ganqiang.datatunnel.meta.redis;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.util.StringUtil;

public class RedisExecutor {

	private static final Logger logger = Logger.getLogger(RedisExecutor.class);

	private String poolId;

	public RedisExecutor(String poolId) {
		this.poolId = poolId;
	}

	public synchronized Jedis getJedis() {
		JedisPool jp = Constants.redis_map.get(poolId);
		try {
			if (jp != null) {
				Jedis resource = jp.getResource();
				return resource;
			}
		} catch (Exception e) {
			logger.error("redis have a fault.", e);
		}
		return null;
	}

	public void mset(Map<String,String> map) {
		Jedis jedis = getJedis();
		String str = "";
		for (String key : map.keySet()) {
			str += key + "," + map.get(key) + ",";
		}
		if (!StringUtil.isNullOrBlank(str)) {
			jedis.mset(str.substring(0, str.length() - 1));
		}
	}
	
	

}
