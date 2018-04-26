package com.bytegriffin.datatunnel.meta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.rabbitmq.client.*;

public class RabbitMQContext implements Initializer {

	private static final Logger logger = LogManager.getLogger(RabbitMQContext.class);

	@Override
	public void init(OperatorDefine operator) {
		ConnectionFactory factory = new ConnectionFactory();
		//factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");
		factory.setHost(operator.getAddress());//本地就写localhost
		Connection connection;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			//topic就是tableName
			channel.queueDeclare(SqlMapper.getTableName(operator.getValue()), false, false, false, null);
			Globals.setRabbitMQChannel(operator.getKey(), channel);
			logger.info("任务[{}]加载组件RabbitMQContext[{}]的初始化完成。", operator.getName(), operator.getId());
		} catch (Exception e) {
			logger.error("任务[{}]加载组件RabbitMQContext[{}]初始化失败。", operator.getName(), operator.getId(), e);
		}
	}

}
