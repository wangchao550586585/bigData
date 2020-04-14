package com.freered.hadoop.mapperduce.wordcount;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;

/**
 * word ： {1.txt:2,2.txt:3} 统计单词在各个文件中出现次数
 * 
 * @author Administrator
 *
 */
public class WcAndIK2 {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage : wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(configuration, "ik");

		job.setJarByClass(WcAndIK2.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(IntSumReducer.class);

		// Map接收的K/V类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[(otherArgs.length - 1)]));
		System.exit((job.waitForCompletion(true)) ? 0 : 1);

	}

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object offset, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 获取文件信息
			/*		FileSplit split = (FileSplit) context.getInputSplit();
					int splitIndex = split.getPath().toString().indexOf("file");
					// 获取文件名称
					String fileName = split.getPath().toString().substring(splitIndex);*/

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();

			IKSegmentation ikSegmentation = new IKSegmentation(
					new InputStreamReader(new ByteArrayInputStream(
							value.getBytes())), true);
			Lexeme t;
			while ((t = ikSegmentation.next()) != null) {
				/*	组装
				 * key				value
				   keyword:1.txt 	1
				*/
				context.write(new Text(t.getLexemeText() + ":" + fileName),
						new Text("1"));
			}

		}
	}

	public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/*
			 *接收的为： word:1.txt list{1,1,1,1}
			 * 变换如下
			 * key 			value
			 * word:1.txt   6
			 * word:2.txt 	3
			 * word:3.txt 	2
			 */
			int sum = 0;
			for (Text value : values) {
				sum += Integer.parseInt(new String(value.getBytes()));
			}
			String[] split = key.toString().split(":");
			String outKey = split[0];
			String outValue = split[1] + ":" + sum;
			context.write(new Text(outKey), new Text(outValue));
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			/* 接收的为word:2.txt 	3
			 * key	 value
			 * word  {1.txt:2,2.txt:3}*/

			StringBuffer stringBuffer = new StringBuffer();
			for (Text text : values) {
				stringBuffer.append("\t");
				stringBuffer.append("-");
				stringBuffer.append(new String(text.getBytes()));
			}
			context.write(key, new Text(stringBuffer.toString()));

		}
	}

}
