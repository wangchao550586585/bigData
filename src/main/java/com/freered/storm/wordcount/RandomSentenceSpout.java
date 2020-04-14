package com.freered.storm.wordcount;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 伪造数据源，在storm框架调用nextTuple()时，发送英文句子出去
 * 
 * @author Administrator
 *
 */
public class RandomSentenceSpout extends BaseRichSpout {
	private static final long serialVersionUID = -2277607445795762286L;
	
	// 用来收集spout输出的tuple
	SpoutOutputCollector collector;
	Random random;

	// 该方法调用一次，主要由storm框架传入SpoutOutputCollector
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		random = new Random();
	}

	// 该方法会被循环调用
	// while(true){
	// nextTuple()
	// }
	public void nextTuple() { 
		String[] sentences = new String[] { "the cow jumped over the moon",
				"an apple a day keeps the doctor away",
				"four score and seven years ago",
				"snow white and the seven dwarfs", "i am at two with nature" };
		String sentence = sentences[random.nextInt(sentences.length)];
		collector.emit(new Values(sentence));
	}

	/**
	 * 消息源可以发射多条消息流stream，多条消息流可以理解为多种类型的数据
	 * 声明此拓扑的所有流的输出模式。
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
