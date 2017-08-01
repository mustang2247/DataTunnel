package com.bytegriffin.datatunnel.read;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.KafkaContext;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.google.common.collect.Lists;

/**
 * 消费者负责从Kafka中读取数据
 */
public class KafkaConsumeReader implements Readable {

	private static final Logger logger = LogManager.getLogger(KafkaConsumeReader.class);

	@Override
	public void channelRead(HandlerContext ctx, Param msg) {
		Properties props = Globals.getKafkaProperties(this.hashCode());
		OperatorDefine opt = Globals.operators.get(this.hashCode());
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		String topic = getTopicName(opt.getValue());
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(KafkaContext.batch_size);
            if(records.isEmpty()){
            	continue;
            }
            List<Record> results = translate(records);
    		msg.setResults(results);
    		ctx.write(msg);
    		logger.info("线程[{}]调用KafkaConsumeReader执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
        }
	}
	
	/**
	 * 获取topic名称
	 * 格式：select * from topic_name
	 * @param sql
	 * @return
	 */
	private String getTopicName(String sql){
		return SqlMapper.select(sql).getTableName();
	}

	/**
	 * 转换record格式
	 * @param records
	 * @return
	 */
	private List<Record> translate(ConsumerRecords<String, String> records){
		List<Record> list = Lists.newArrayList();
        for (ConsumerRecord<String, String> consumerRecord : records){
        	Record record = new Record();
    		List<Field> fields = Lists.newArrayList();
        	fields.add(new Field(KafkaContext.topic, consumerRecord.topic()));
        	fields.add(new Field(KafkaContext.key, consumerRecord.key()));
        	fields.add(new Field(KafkaContext.value, consumerRecord.value()));
        	fields.add(new Field(KafkaContext.offset, consumerRecord.offset()));
        	record.setFields(fields);
            list.add(record);
        }
        return list;
	}

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++){
			producer.send(new ProducerRecord<>("topic1", Integer.toString(i), Integer.toString(i)));
			System.out.println("send成功");
		}
		
		producer.close();
	}


}
