package com.ganqiang.datatunnel.api.example;

import redis.clients.jedis.Jedis;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.redis.RedisExecutor;

public class RedisWriteExample implements Writeable{

	@SuppressWarnings("unused")
	@Override
	public void write(Param param) {
		Pair pair = param.getPair();
		RedisExecutor exe = new RedisExecutor(pair.getWriterPoolId());
		Jedis jedis = (Jedis) param.getReadResult();
//		System.out.println(jedis.lrange("a", 0,12));//lpush
		System.out.println(jedis.sinter("aaa","bbb"));// sadd
	}

}
