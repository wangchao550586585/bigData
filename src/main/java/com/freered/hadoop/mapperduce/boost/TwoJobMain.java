package com.freered.hadoop.mapperduce.boost;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoJobMain {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("yarn.resourcemanager.hostname", "hadoop01");
			Job job = Job.getInstance(conf, "weibo2");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setMapperClass(TwoMapper.class);
			job.setCombinerClass(TwoReduce.class);
			job.setReducerClass(TwoReduce.class);

			// 读取firstJob工作的结果目录，进行计算
			FileInputFormat.addInputPath(job, new Path("/sanbox/output01"));
			FileOutputFormat.setOutputPath(job, new Path("/sanbox/output02"));
			System.exit((job.waitForCompletion(true)) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 统计每个词在所有微博出现的次数：DF
	 * 
	 * @author Administrator
	 *
	 */
	static class TwoMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			FileSplit fs = (FileSplit) context.getInputSplit();
			if (!fs.getPath().getName().contains("part-r-00003")) {// 统计的总微博条数
				String[] val = value.toString().trim().split("\t");
				if (val.length >= 2) {
					String[] v = val[0].split("-");
					if (v.length >= 2) {
						//统计DF，该词在那些微博中出现过
						context.write(new Text(v[0]), new IntWritable(1));
					}
				}
			} else {
				System.out.println(value.toString() + "-------------");
			}
		}
	}

	static class TwoReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> iterable,
				Context context) throws IOException, InterruptedException {
			int num = 0;
			for (IntWritable i : iterable) {
				num += i.get();
			}
			context.write(key, new IntWritable(num));
		}
	}

}
