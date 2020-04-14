package com.freered.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * 通过javaAPI操作hdfs文件系统 Created by Administrator on 2016/12/30.
 */
public class HdfsMain {
	static FileSystem fs;

	public static void main(String[] args) throws Exception {
		// printConfig();
		getFS();
		fileList();
		delete();
		mkdir();
		upload();
		rename();
		download();
	}

	private static void getFS() throws Exception {
		/*    下面代码在windows和非hadoop集群用户组电脑上，会出错，
		 Configuration conf=   new Configuration();
		  conf.set("dfs.replication", "2");
		  conf.set("dfs.defaultFS", "hdfs://hadoop01:9000");
		  FileSystem fs =  FileSystem.get(conf);*/

		// 配置通过当前客户端操作hdfs时的配置信息，会覆盖hadoop默认配置
		Configuration conf = new Configuration();
		// 设置当前客户端存储文件时，要分割的block文件块数量
		conf.set("dfs.replication", "1");
		// 支持多种文件系统，默认hdfs，支持ftp，本地文件系统
		// fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
		fs = FileSystem.get(new URI("hdfs://hadoop01:8082"), conf, "root");
	}

	public static void download() throws Exception {
		// 下载
		Path hadoopPath = new Path("/upload");
		Path descPath2 = new Path("C:/Users/Administrator/Desktop");
		fs.copyToLocalFile(hadoopPath, descPath2);
		// 如果不使用winutils.exe会报错，使用如下代码
		/*是否删除HDFS集群下的原始文件，HDFS文件地址,本地文件地址，是否使用本地文件系统操作*/
		// fs.copyToLocalFile(false, hadoopPath, descPath, true);

	}

	private static void rename() throws IOException {
		// 移动文件 or 重命名文件
		fs.rename(new Path("/upload/zookeeper-3.4.9.tar.gz"), new Path(
				"/upload/zookeeper.tar.gz"));
	}

	private static void upload() throws IOException {
		// 上传
		Path srcPath = new Path(
				"C:\\Users\\Administrator\\Desktop\\hadoop\\software\\zookeeper-3.4.9.tar.gz");
		Path descPath = new Path("/upload");
		fs.copyFromLocalFile(srcPath, descPath);
	}

	private static void mkdir() throws IOException {
		// 创建目录
		boolean tag = fs.mkdirs(new Path("/upload"));
		System.out.println(tag);
	}

	private static void delete() throws IOException {
		// 递归删除目录下文件信息
		fs.delete(new Path("/upload"), true);
	}

	// 查看HDFS上的文件信息
	public static void fileList() throws Exception {
		RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(
				new Path("/"), true);
		while (fileList.hasNext()) {
			LocatedFileStatus locatedFileStatus = fileList.next();
			// 获取文件权限
			locatedFileStatus.getPermission().toString();
			// 文件所属用户
			locatedFileStatus.getOwner();
			// 文件大小
			locatedFileStatus.getLen();
			// 文件时间戳
			locatedFileStatus.getModificationTime();
			// 文件路径
			locatedFileStatus.getPath();
			for (BlockLocation blockLocation : locatedFileStatus
					.getBlockLocations()) {
				for (String hosts : blockLocation.getCachedHosts()) {
					// 打印block的cachehosts
					System.out.println(hosts);

				}
				for (String hosts : blockLocation.getHosts()) {
					// 打印文件block所在的服务器
					System.out.println(hosts);
				}
				// 文件大小
				blockLocation.getLength();
				// 偏移位置
				blockLocation.getOffset();
			}

		}
	}

	/**
	 * 查看hdfs默认配置文件
	 */
	public static void printConfig() {
		Configuration conf = new Configuration();
		Iterator<Map.Entry<String, String>> it = conf.iterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}
}
