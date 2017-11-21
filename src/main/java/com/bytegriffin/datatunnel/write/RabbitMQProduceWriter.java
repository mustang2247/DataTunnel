package com.bytegriffin.datatunnel.write;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.InsertObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.bytegriffin.datatunnel.util.SerializeUtil;
import com.rabbitmq.client.Channel;

/**
 * 消息生产者
 * 注意：额外定义了一个Record序列化对象存储Message
 */
public class RabbitMQProduceWriter implements Writeable {

	private static final Logger logger = LogManager.getLogger(RabbitMQProduceWriter.class);

	@Override
	public void channelRead(HandlerContext ctx, Param msg) {
		Channel channel = Globals.getRabbitMQChannel(this.hashCode());
		OperatorDefine opt = Globals.operators.get(this.hashCode());
		List<String> sqls = SqlParser.getWriteSql(msg.getRecords(), opt.getValue());
		write(channel, sqls);
		logger.info("线程[{}]调用RabbitMQProduceWriter执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
	}

	/**
	 * 目前只支持insert操作
	 *
	 * @param properties
	 */
	private void write(Channel channel, List<String> sqls) {
		try {
			String firstSql = sqls.get(0).toLowerCase().trim();
			if (firstSql.contains("insert")) {
				for(String sql : sqls) {
					InsertObject insertObject = SqlMapper.insert(sql);
					List<Field> fields = insertObject.getFields();			
					channel.basicPublish("", insertObject.getTableName(), null, SerializeUtil.serialize(new Record(fields)));
				}
			} else {
				logger.warn("线程[{}]调用RabbitMQProduceWriter只支持insert操作", Thread.currentThread().getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
