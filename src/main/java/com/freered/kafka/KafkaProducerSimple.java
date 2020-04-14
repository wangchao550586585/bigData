package com.freered.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 生产消息， 需要配置hosts/kafka
 * 
 * @author Administrator
 *
 */
public class KafkaProducerSimple {

	private final static String TOPIC = "freered";

	public static void main(String[] args) {
		// 配置consumer.properties
		Properties properties = new Properties();
		//kafka.serializer.StringEncoder设置相应的序列化类型
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("metadata.broker.list",
				"kafka01:9092,kafka02:9092,kafka03:9092");
		properties.put("request.required.acks", "1");

		Producer<Integer,String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
		int messageNo = 1;
		while (true) {
			String string = new String("Message" + messageNo);
			producer.send(new KeyedMessage<Integer,String>(TOPIC, string));
			messageNo++;
		}

	}
}
