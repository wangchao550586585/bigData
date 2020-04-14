package com.freered.storm.double11;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.freered.storm.double11.domain.PaymentInfo;

public class PaymentInfoSpout extends BaseRichSpout {
	private static final long serialVersionUID = -8555114249399575832L;

	private SpoutOutputCollector collector;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		collector.emit(new Values(new PaymentInfo().random()));
		// /Utils.sleep(500);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("paymentInfo"));
	}

}
