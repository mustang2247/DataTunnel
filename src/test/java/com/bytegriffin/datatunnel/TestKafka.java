package com.bytegriffin.datatunnel;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 1.启动内置zookeeper
 * ./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
 * 2.启动kafka
 * ./bin/kafka-server-start.sh -daemon ./config/server.properties
 * 3.创建一个名为"test"的topic
 * ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
 * 4.生产消息
 * ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
 * 5.消费消息
 * ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
 */
public class TestKafka {

	private static String topic = "kafka_topic";

	private static void produce() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++){
			producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
			System.out.println("生产成功："+Integer.toString(i));
		}

		producer.close();
	}

	private static void consume() {
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG ,"test") ;
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        @SuppressWarnings("resource")
		Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> System.out.println("消费成功："+record.value()));
        }
	}

	public static void main(String[] args) {
		produce();
		consume();
	}

}
