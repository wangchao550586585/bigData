package com.freered.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopologyMain {
	public static void main(String[] args) throws Exception {
		// storm框架语言，支持多种语言，在java需要使用此对象构建
		TopologyBuilder builder = new TopologyBuilder();
		// 在已知的句子中，随机发送一句出去，4个线程执行
		builder.setSpout("spout", new RandomSentenceSpout(), 4);
		// 将一行一行文本内容切割成单词，任务数12
		builder.setBolt("split", new SplitSentenceBolt(), 12).shuffleGrouping(
				"spout");
		// 负责将单词出现频率累加
		builder.setBolt("count", new WordCountBolt(), 3).fieldsGrouping(
				"split", new Fields("word"));

		// builder.setBolt("newBolt",new
		// WordCountBolt(),5).shuffleGrouping("spout").shuffleGrouping("source");

		// 启动topology的配置信息
		Config config = new Config();
		// TOPOLOGY_DEBUG(setDebug), 当它被设置成true的话， storm会记录下每个组件所发射的每条消息。
		// 这在本地环境调试topology很有用， 但是在线上这么做的话会影响性能的。
		config.setDebug(true);

		// storm有2种运行模式，本地模式和分布式模式
		if (args != null && args.length > 0) {
			// 希望集群分配多少个工作进程执行这个topology
			config.setNumWorkers(3);
			// 向集群提交topology
			StormSubmitter.submitTopologyWithProgressBar(args[0], config,
					builder.createTopology());
		} else {
			config.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", config,
					builder.createTopology());
			// Utils.sleep(10000);
			// cluster.shutdown();
		}

	}
}
