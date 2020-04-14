package com.freered.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import clojure.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用文件流方式获取hdfs数据
 * 
 * DFSInputStream#fetchLocatedBlocksAndGetLastBlockLength：可以打印出元数据信息
 * 
 * if (DFSClient.LOG.isDebugEnabled()) { DFSClient.LOG.debug("newInfo = " +
 * newInfo); }
 */
public class HdfsMain2 {
	public static void main(String[] args) {

	}

	public FileSystem getFS() throws IOException, InterruptedException,
			URISyntaxException {
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		return FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
	}

	/**
	 * 使用文件流获取hdfs文件数据
	 * 
	 * @throws Exception
	 */
	public void test1() throws Exception {
		FSDataInputStream fsDataInputStream = getFS().open(
				new Path("hdfs://hadoop:9000/data/hadoop.tar"));
		FileOutputStream f = new FileOutputStream(new File("G:/a.txt"));
		IOUtils.copyBytes(fsDataInputStream, f, 4096);
	}

	/**
	 * 使用文件流方式随机读取一部分数据
	 * 
	 * @throws Exception
	 */
	public void test2() throws Exception {
		FSDataInputStream fsDataInputStream = getFS().open(
				new Path("hdfs://hadoop01:9000/data/a.txt"));
		// 设置偏移量
		fsDataInputStream.seek(134217728);
		FileOutputStream fileOutputStream = new FileOutputStream(new File(
				"G://a.txt"));
		IOUtils.copyBytes(fsDataInputStream, fileOutputStream, 4096, true);
	}

	/**
	 * 使用文件流方式读取第一个文件块
	 * 
	 * @throws Exception
	 */
	public void test3() throws Exception {
		FSDataInputStream fsDataInputStream = getFS().open(
				new Path("hdfs://hadoop01:9000/data/a.txt"));
		FileOutputStream fileOutputStream = new FileOutputStream(new File(
				"G://a.txt"));
		// 设置偏移量
		long offset = 0;
		byte b[] = new byte[4096];
		// 读取文件数据流，从offset开始，读到buffer中，从buffer角标0的位置开始存放，存放4096个
		while (fsDataInputStream.read(offset, b, 0, 4096) != -1) {
			if (offset >= 134217728) {
				return;
			}
			fileOutputStream.write(b);
			offset += 4096;
		}
		fileOutputStream.flush();
		fsDataInputStream.close();
		fileOutputStream.close();
	}

	/**
	 * 逐行读取文本数据
	 * 
	 * @throws Exception
	 */
	public void test4() throws Exception {
		FSDataInputStream fsDataInputStream = getFS().open(
				new Path("hdfs://hadoop01:9000/data/a.txt"));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fsDataInputStream));
		String line = null;
		while ((line = reader.readLine()) != null) {
			System.out.println(line);
		}
		fsDataInputStream.close();
	}

	/**
	 * 文件打印控制台
	 * 
	 * @throws Exception
	 */
	public void test5() throws Exception {
		FSDataInputStream fsDataInputStream = getFS().open(
				new Path("hdfs://hadoop01:9000/data/a.txt"));
		IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
	}

}
