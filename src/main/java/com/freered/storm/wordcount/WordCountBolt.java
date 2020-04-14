package com.freered.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class WordCountBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -3452606590014621970L;

	Map<String, Integer> counters = new HashMap<String, Integer>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String string = input.getString(0);
		if (!counters.containsKey(string)) {
			counters.put(string, 1);
		} else {
			counters.put(string, counters.get(string) + 1);
		}
		System.out.println(counters);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
