package com.ganqiang.datatunnel.api.example;

import redis.clients.jedis.Jedis;

import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.redis.RedisExecutor;

public class RedisReadExample implements Readable{

	@Override
	public Object read(Param param) {
		Pair pair = param.getPair();
		RedisExecutor hbe = new RedisExecutor(pair.getReaderPoolId());
		Jedis jedis = hbe.getJedis();
//		jedis.lpush("a", "123");
//		jedis.lpush("a", "bbb");

		jedis.sadd("aaa", "123");
		jedis.sadd("aaa", "bbb");

		jedis.sadd("bbb", "ty");
		jedis.sadd("bbb", ",m");

		return jedis;
	}

}