package com.freered.hadoop.mapperduce.temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区，默认采用hash取模分区到每个reduce上
 * 
 * 根据年份分区
 * @author Administrator
 *
 */
public class FirstPartition extends Partitioner<KeyPair, Text> {

	/**
	 * key/value/reduce数量
	 */
	public int getPartition(KeyPair key, Text value, int numPartitions) {
		// *127为了打散，这样同一年份的肯定在同一个reduce上
		return (key.getYear() * 127) % numPartitions;
	}

}
