package com.freered.storm.double11;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import com.freered.storm.double11.domain.PaymentInfo;
import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Save2RedisBlot extends BaseBasicBolt {
	private JedisPool pool;

	public void prepare(Map stormConf, TopologyContext context) {
		JedisPoolConfig conf = new JedisPoolConfig();
		// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
		conf.setMaxIdle(5);
		// 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
		// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		conf.setMaxTotal(100 * 1000);
		// 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
		conf.setMaxWaitMillis(30);
		conf.setTestOnBorrow(true);
		conf.setTestOnReturn(true);
		/**
		 * 如果你遇到 java.net.SocketTimeoutException: Read timed out exception的异常信息
		 * 请尝试在构造JedisPool的时候设置自己的超时值. JedisPool默认的超时时间是2秒(单位毫秒)
		 */
		pool = new JedisPool(conf, "127.0.0.1", 6379);
		super.prepare(stormConf, context);
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String paymentInfoStr = input.getStringByField("message");
		PaymentInfo paymentInfo = new Gson().fromJson(paymentInfoStr,
				PaymentInfo.class);
		Jedis jedis = pool.getResource();
		if (paymentInfo != null) {
			// 计算订单的总数
			jedis.incrBy("orderTotalNum", 1);
			// 计算总的销售额
			jedis.incrBy("orderTotalPrice", paymentInfo.getProductPrice());
			// 计算折扣后的销售额
			jedis.incrBy("orderPromotionPrice", paymentInfo.getPromotionPrice());
			// 计算实际交易额
			jedis.incrBy("orderTotalRealPay", paymentInfo.getPayPrice());
			jedis.incrBy("userNum", 1);
		}
		System.out.println("订单总数：" + jedis.get("orderTotalNum") + "   销售额"
				+ jedis.get("orderTotalPrice") + "   交易额"
				+ jedis.get("orderPromotionPrice") + "   实际支付："
				+ jedis.get("orderTotalRealPay") + "   下单用户："
				+ jedis.get("userNum"));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
