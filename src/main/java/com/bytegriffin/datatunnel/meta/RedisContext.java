package com.bytegriffin.datatunnel.meta;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisContext implements Initializer {

	private static final Logger logger = LogManager.getLogger(RedisContext.class);

	private static final String host_split = ",";
	private static final int default_port = 6379;

	private static String redisMode = Globals.redis_mode;//默认是单机
	private static String redisAuth;
	
	public static final String list = "list"; //list模式
	public static final String hash = "hash";//hash模式

	@Override
	public void init(OperatorDefine operator) {
		String address = operator.getAddress();
		if (Strings.isNullOrEmpty(redisMode) || Strings.isNullOrEmpty(address)) {
			logger.error("Redis在初始化时参数为空。");
			System.exit(1);
		}
		JedisCommands commands = null;
		if ("one".equalsIgnoreCase(redisMode)) {
			commands = one(address, redisAuth);
		} else if ("sharded".equalsIgnoreCase(redisMode)) {
			commands = sharded(address, redisAuth);
		} else if ("cluster".equalsIgnoreCase(redisMode)) {
			commands = cluster(address, redisAuth);
		}
		Globals.setJedisCommands(operator.getKey(), commands);
		logger.info("任务[{}]加载组件RedisContext[{}]的初始化完成。", operator.getName(), operator.getId());
	}

	private static JedisPoolConfig config() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1000);
		config.setMaxIdle(5);
		config.setMinIdle(1);
		config.setBlockWhenExhausted(true);
		config.setMaxWaitMillis(60 * 1000);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		config.setTestWhileIdle(true);
		config.setTimeBetweenEvictionRunsMillis(60 * 1000);
		return config;
	}

	/**
	 * 关闭链接
	 * @param jedis
	 */
	public  static void close(JedisCommands jedis) {
		if ("one".equalsIgnoreCase(redisMode)) {
			Jedis one = (Jedis) jedis;
			if(one != null){
				one.close();
			}
		} else if ("sharded".equalsIgnoreCase(redisMode)) {
			ShardedJedis sharedJedis = (ShardedJedis)jedis;
			if(sharedJedis != null){
				sharedJedis.close();
			}
		} else if ("cluster".equalsIgnoreCase(redisMode)) {
			try {
				JedisCluster  jedisCluster = (JedisCluster) jedis;
				if(jedisCluster != null){
					jedisCluster.close();
				}
			} catch (IOException e) {
				logger.error("Redis关闭链接失败。", e);
			}
		}
	}

	/**
	 * 单机
	 */
	@SuppressWarnings("resource")
	public JedisCommands one(String address, String password) {
		String firstAddress = Splitter.on(host_split).trimResults().split(address).iterator().next();
		List<String> list = Splitter.on(":").trimResults().splitToList(firstAddress);
		String host = "";
		Integer port = null;
		if (list.size() == 1) {
			host = Strings.isNullOrEmpty(list.get(0)) ? null : list.get(0);
			port = default_port;
		} else if (list.size() == 2) {
			host = Strings.isNullOrEmpty(list.get(0)) ? null : list.get(0);
			port = Strings.isNullOrEmpty(list.get(1)) ? null : Integer.valueOf(list.get(1));
		} else {
			logger.error("Redis地址格式有问题：{}", address);
			System.exit(1);
		}
		JedisPool jedisPool = null;
		if (Strings.isNullOrEmpty(password)) {
			jedisPool = new JedisPool(config(), host, port);
		} else {
			jedisPool = new JedisPool(config(), host, port, 2000, password);
		}
		try {
			return jedisPool.getResource();
		} catch (Exception e) {
			logger.error("Redis初始化链接失败。", e);
			System.exit(1);
		}
		return null;
	}

	/**
	 * 分片 客户端集群
	 */
	public JedisCommands sharded(String addresses, String password) {
		List<String> list = Splitter.on(host_split).trimResults().splitToList(addresses);
		List<JedisShardInfo> shards = Lists.newArrayList();
		for (String addr : list) {
			List<String> hostAndPort = Splitter.on(":").trimResults().splitToList(addr);
			if (hostAndPort.size() == 1) {
				JedisShardInfo node = new JedisShardInfo(hostAndPort.get(0), default_port);
				if (!Strings.isNullOrEmpty(password)) {
					node.setPassword(password);
				}
				shards.add(node);
			} else if (hostAndPort.size() == 2) {
				JedisShardInfo node = new JedisShardInfo(hostAndPort.get(0), Integer.valueOf(hostAndPort.get(1)));
				if (!Strings.isNullOrEmpty(password)) {
					node.setPassword(password);
				}
				shards.add(node);
			}
		}
		if (shards == null || shards.isEmpty()) {
			logger.error("Redis地址格式有问题。");
			System.exit(1);
		}
		@SuppressWarnings("resource")
		ShardedJedisPool shardedJedisPool = new ShardedJedisPool(config(), shards);
		ShardedJedis sharedJedis = null;
		try {
			sharedJedis = shardedJedisPool.getResource();
			return sharedJedis;
		} catch (Exception e) {
			logger.error("Redis初始化链接失败。", e);
			System.exit(1);
		}
		return null;
	}

	/**
	 * 集群 redis 3以上支持cluster
	 */
	public JedisCommands cluster(String addresses, String password) {
		List<String> list = Splitter.on(host_split).trimResults().splitToList(addresses);
		Set<HostAndPort> clusterNodes = Sets.newHashSet();
		for (String addr : list) {
			List<String> hostAndPort = Splitter.on(":").trimResults().splitToList(addr);
			if (hostAndPort.size() == 1) {
				clusterNodes.add(new HostAndPort(hostAndPort.get(0), default_port));
			} else if (hostAndPort.size() == 2) {
				clusterNodes.add(new HostAndPort(hostAndPort.get(0), Integer.valueOf(hostAndPort.get(1))));
			}
		}
		if (clusterNodes == null || clusterNodes.isEmpty()) {
			logger.error("Redis地址格式有问题。");
			System.exit(1);
		}
		JedisCluster  jedisCluster = null;
		try {
			if (Strings.isNullOrEmpty(redisAuth)) {
				jedisCluster = new JedisCluster(clusterNodes, 2000, 2000, 5, config());
			} else {
				jedisCluster = new JedisCluster(clusterNodes, 2000, 2000, 5, redisAuth, config());
			}
			return jedisCluster;
		} catch (Exception e) {
			logger.error("Redis初始化链接失败。", e);
			System.exit(1);
		} 
		return null;
	}

}
