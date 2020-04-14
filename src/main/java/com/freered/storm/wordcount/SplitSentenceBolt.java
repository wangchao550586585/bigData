package com.freered.storm.wordcount;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 8004395256928180245L;

	// 该方法只会被调用一次，用来初始化
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
	}

	/**
	 * @Tuple 接受的句子
	 * 
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentences = input.getString(0);
		String[] split = sentences.split(" ");
		for (String word : split) {
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
