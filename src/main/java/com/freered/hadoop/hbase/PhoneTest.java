package com.freered.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;
import org.junit.jupiter.api.Test;

public class PhoneTest {
	@Test
	public void create() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");
		HBaseAdmin admin = new HBaseAdmin(config);
		String table = "t_cdr";
		if (admin.isTableAvailable(table)) {
			admin.disableTable(table);
			admin.deleteTable(table);
		} else {
			HTableDescriptor t = new HTableDescriptor(table.getBytes());
			HColumnDescriptor cf1 = new HColumnDescriptor("cf1");
			// cf1.setMaxVersions(10);
			t.addFamily(cf1);
			admin.createTable(t);
		}
		admin.close();
	}

	@Test
	public void add() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");
		HTable hTable = new HTable(config, "t_cdr");
		String rowKey = "123456798_" + System.currentTimeMillis();
		Put put = new Put(rowKey.getBytes());
		put.add("cf1".getBytes(), "dest".getBytes(), "13277326416".getBytes());
		put.add("cf1".getBytes(), "type".getBytes(), "1".getBytes());
		put.add("cf1".getBytes(), "time".getBytes(), "2017-2-14".getBytes());
		hTable.put(put);
		hTable.close();
	}

	@Test
	public void scan() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");
		HTable table = new HTable(config, "t_cdr");
		Get get = new Get("123456798_1487136452643".getBytes());
		Result result = table.get(get);
		Cell c1 = result.getColumnLatestCell("cf1".getBytes(),
				"type".getBytes());
		Cell c2 = result.getColumnLatestCell("cf1".getBytes(),
				"dest".getBytes());
		Cell c3 = result.getColumnLatestCell("cf1".getBytes(),
				"time".getBytes());
		System.out.println(String.valueOf(c1) + String.valueOf(c2)
				+ String.valueOf(c3));

		// rowkey字典排序
		Scan scan = new Scan();
		//不设置的话，默认查询所有，设置的话则查询rowkey范围内的值
		scan.setStartRow("123456798_1487136452643".getBytes());
		scan.setStopRow("123456798_999999999999999".getBytes());
		
		ResultScanner scanner = table.getScanner(scan);
		for(Result r:scanner){
		}
		table.close();
	}

}
