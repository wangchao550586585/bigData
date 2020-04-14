package com.freered.storm.double11;

import java.util.Calendar;
import java.util.Date;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.freered.storm.double11.domain.PaymentInfo;
import com.google.gson.Gson;

public class FilterMessageBlot extends BaseBasicBolt {

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String paymentInfo = input.getStringByField("paymentInfo");
		PaymentInfo fromJson = new Gson().fromJson(paymentInfo,
				PaymentInfo.class);
		Date date = fromJson.getCreateOrderTime();
		// 过滤订单时间,如果订单时间在2015.11.11这天才进入下游开始计算
		if (Calendar.getInstance().get(Calendar.DAY_OF_MONTH) != 4) {
			return;
		}
		collector.emit(new Values(paymentInfo));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
