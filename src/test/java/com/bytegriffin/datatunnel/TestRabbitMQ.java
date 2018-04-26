package com.bytegriffin.datatunnel;

import java.io.IOException;

import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.util.SerializeUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class TestRabbitMQ {

	private final static String QUEUE_NAME = "page";

	private static void send() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		// factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		String message = "Hello World!";
		channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
		System.out.println("Sent '" + message + "'");

		channel.close();
		connection.close();
	}

	private static void receive() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				Record record = SerializeUtil.deserialize(body);
				System.out.println("Received '" + record + "'");
			}
		};
		channel.basicConsume(QUEUE_NAME, true, consumer);
		channel.close();
		connection.close();
	}

	public static void main(String[] argv) throws Exception {
		send();
		receive();
	}

}
