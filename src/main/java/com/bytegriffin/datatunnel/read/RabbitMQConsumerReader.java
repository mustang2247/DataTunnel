package com.bytegriffin.datatunnel.read;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.SelectObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.bytegriffin.datatunnel.util.SerializeUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 消息消费者
 * 注意：额外定义了一个Record序列化对象存储Message
 */
public class RabbitMQConsumerReader implements Readable {

	private static final Logger logger = LogManager.getLogger(RabbitMQConsumerReader.class);

	@Override
	public void channelRead(HandlerContext ctx, Param msg) {
		Channel channel = Globals.getRabbitMQChannel(this.hashCode());
		OperatorDefine opt = Globals.operators.get(this.hashCode());
        String newsql = SqlParser.getReadSql(opt.getValue());
        List<Record> results = select(channel, newsql);
        msg.setRecords(results);
        ctx.write(msg);
        logger.info("线程[{}]调用RabbitMQConsumerReader执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
	}

	/**
	 * 查找某主题（表名）下的消息
	 * @param channel
	 * @param sql
	 * @return
	 */
	private List<Record> select(Channel channel, String sql) {
		if (!channel.isOpen() || Strings.isNullOrEmpty(sql)) {
            return null;
        }
		SelectObject select = SqlMapper.select(sql);
        List<Record> list = Lists.newArrayList();
        try {
        	Consumer consumer = new DefaultConsumer(channel) {
    			@Override
    			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
    					byte[] body) throws IOException {
    				Record record = SerializeUtil.deserialize(body);
    				list.add(record);
    			}
    		};
    		channel.basicConsume(select.getTableName(), true, consumer);
        	return list;
        } catch (Exception e) {
            logger.error("RabbitMQConsumerReader查询数据时出错: {}", sql, e);
        }
        return list;
	}

}
