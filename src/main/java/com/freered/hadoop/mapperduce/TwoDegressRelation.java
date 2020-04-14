package com.freered.hadoop.mapperduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 2度关系
 * 
			ab
			bc
			cd
			ed
			ef
			结果为：
					a       c
					c       a
					b       d
					d       b
					c       e
					e       c
					d       f
					f       d

 * @author Administrator
 *
 */
public class TwoDegressRelation {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJobName("2DegressRelation");
		job.setJarByClass(TwoDegressRelation.class);
		job.setMapperClass(TwoDegressRelationMapper.class);
		// 不需要
		// job.setCombinerClass(TwoDegressRelationReduce.class);
		job.setReducerClass(TwoDegressRelationReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//自定义传参，不需要指定输入输出目录
		FileInputFormat.addInputPath(job, new Path("/sanbox"));
		FileOutputFormat.setOutputPath(job, new Path("/sanbox/output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TwoDegressRelationMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] ss = value.toString().split(" ");
			context.write(new Text(ss[0]), new Text(ss[1]));
			context.write(new Text(ss[1]), new Text(ss[0]));
		}
	}

	public static class TwoDegressRelationReduce extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text value, Iterable<Text> iterable,
				Context context) throws IOException, InterruptedException {
			// 去重
			Set<String> set = new HashSet<String>();
			for (Text text : iterable) {
				set.add(text.toString());
			}
			/*
			 * 建立笛卡尔积
			 */
			if (set.size() > 1) {
				for (String str1 : set) {
					for (String str2 : set) {
						if (!str1.equals(str2)) {
							context.write(new Text(str1), new Text(str2));
						}
					}
				}
			}

		}
	}
}
