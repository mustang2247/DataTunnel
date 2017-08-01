package com.bytegriffin.datatunnel.meta;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;

public class KafkaContext implements Initializer {

	private static final Logger logger = LogManager.getLogger(KafkaContext.class);

	public static final String key = "key";
	public static final String value = "value";
	public static final String topic = "topic";
	public static final String offset = "offset";
	public static final int batch_size = 100;

	@Override
	public void init(OperatorDefine operator) {
		try {
			String address = operator.getAddress();
			Properties props = new Properties();
			if (operator.isReader()) {
				props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
				props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id_" + operator.getId());
				props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
				props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
				props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
				props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringDeserializer");
				props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringDeserializer");
			} else if (operator.isWriter()) {
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
				props.put(ProducerConfig.ACKS_CONFIG, "all");
				props.put(ProducerConfig.RETRIES_CONFIG, 0);
				props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
				props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
				props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer");
				props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer");
			}
			Globals.setKafkaProperties(operator.getKey(), props);
			logger.info("任务[{}]加载组件KafkaContext[{}]的初始化完成。", operator.getName(), operator.getId());
		} catch (RuntimeException re) {
			logger.error("任务[{}]加载组件KafkaContext[{}]没有连接成功。", operator.getName(), operator.getId(), re);
		}
	}

}
