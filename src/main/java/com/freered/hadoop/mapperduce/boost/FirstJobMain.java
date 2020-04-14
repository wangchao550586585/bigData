package com.freered.hadoop.mapperduce.boost;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;

/**
 * 广告推送
 * 在新浪微博中精确推送广告
 * W=TF*Log(N/DF)
 * TF:当前关键字在该条微博中出现次数
 * N:微博总数
 * DF:当前关键字在所有微博中出现的次数
 * 
 * 统计TF&N
 * @author Administrator
 * 
 * 3823890201582094	今天我约了豆浆，油条。约了电饭煲几小时后饭就自动煮好，还想约豆浆机，让我早晨多睡一小时，豆浆就自然好。起床就可以喝上香喷喷的豆浆了。
   3823890210294392	今天我约了豆浆，油条
 *
 */
public class FirstJobMain {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("yarn.resourcemanager.hostname", "hadoop01");
			Job job = Job.getInstance(conf);
			job.setJarByClass(FirstJobMain.class);
			job.setJobName("weibo01");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setNumReduceTasks(3);
			//分区，那些数据在哪个reduce
			job.setPartitionerClass(FirstPartition.class);
			job.setMapperClass(FirstMapper.class);
			job.setCombinerClass(FirstReduce.class);
			job.setReducerClass(FirstReduce.class);

			FileInputFormat.addInputPath(job, new Path("/sanbox/input"));
			FileOutputFormat.setOutputPath(job, new Path("/sanbox/output01"));
			System.exit((job.waitForCompletion(true)) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 自定义分区
	 * @author Administrator
	 *
	 */
	static class FirstPartition extends HashPartitioner<Text, IntWritable> {

		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			/*
			 * count统计要单独文件，否则和别的数据在一块不方便查看，又因为一个reduce生成一个文件，所以在此指定count专属的reduce
			 * 这样，reduce-1分给剩余的job
			 */
			return key.equals(new Text("count")) ? 2 : super.getPartition(key,
					value, numReduceTasks - 1);
		}
	}

	static class FirstMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] v = value.toString().trim().split("\t");
			if (v.length >= 2) {
				String id = v[0].trim();
				String content = v[1].trim();
				StringReader stringReader = new StringReader(content);
				IKSegmentation ikSegmentation = new IKSegmentation(
						stringReader, true);
				Lexeme word = null;
				while ((word = ikSegmentation.next()) != null) {
					String text = word.getLexemeText();
					// 统计TF,该字段在该条微博中出现次数
					context.write(new Text(text + "_" + id), new IntWritable(1));
				}
				// 统计微博总条数
				context.write(new Text("count"), new IntWritable(1));
			} else {
				System.out.println(value.toString() + "===============");
			}
		}
	}

	static class FirstReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> iterable,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : iterable) {
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));

			// 输出count的统计个数
			if (key.equals(new Text("count"))) {
				System.out.println(key.toString() + "-----------------");
			}
		}
	}
}
