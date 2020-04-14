package com.freered.hadoop.mapperduce.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序
 * 
 * @author Administrator1
 *
 */
public class SortTemperature extends WritableComparator {

	public SortTemperature() {
		/* 1:传入比较对象
		 * 2:是否创建实例
		 */
		super(KeyPair.class, true);
	}

	/**
	 * 比较，为了排序 年份asc，温度desc
	 */
	public int compare(WritableComparable a, WritableComparable b) {
		KeyPair o1 = (KeyPair) a;
		KeyPair o2 = (KeyPair) b;
		int compare = Integer.compare(o1.getYear(), o2.getYear());
		if (compare != 0)
			return compare;
		return -Integer.compare(o1.getTemperature(), o2.getTemperature());

	}
}
