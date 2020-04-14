package com.freered.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 消费数据，还需要配置hosts/zk01,zk02,zk03
 * 
 * @author Administrator
 *
 */
public class KafkaConsumerSimple {
	private final static String TOPIC = "freered";

	public static void main(String[] args) {
		// producer.propertis配置文件
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "zk01:2181,zk02:2181,zk03:2181");
		properties.put("group.id", "xxx");
		properties.put("zookeeper.session.timeout.ms", "400");
		properties.put("zookeeper.sync.time.ms", "200");
		properties.put("auto.commit.interval.ms", "1000");

		// 创建consumer
		ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(properties));
		// 消费数据
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams = consumer
				.createMessageStreams(topicCountMap);

		KafkaStream<byte[], byte[]> kafkaStream = createMessageStreams.get(
				TOPIC).get(0);
		ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
		while (iterator.hasNext()) {
			System.out.println(new String(iterator.next().message()));
		}

	}
}
