package com.freered.hadoop.mapperduce.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组，默认采用年份和温度相同才分组在一起，这里自定义，年份相同分在一起
 * 
 * @author Administrator
 *
 */
public class GroupTemperature extends WritableComparator {

	public GroupTemperature() {
		super(KeyPair.class, true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		KeyPair o1 = (KeyPair) a;
		KeyPair o2 = (KeyPair) b;
		return Integer.compare(o1.getYear(), o2.getYear());

	}
}
