package com.freered.hadoop.mapperduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 驱动类 单词计数
 * 
 * @author Administrator
 *
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage : wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		// 设置任务名称
		Job job = new Job(configuration, "word count");

		job.setJarByClass(WordCount.class);

		// 修改1： 将TokenizerMapper修改成自己的类
		job.setMapperClass(TokenizerMapper.class);
		// Combiner是在maptask端处理执行的Reducer任务，叫做本地reduce
		// 意义：对每一个maptask的输出进行局部汇总，较少网络传输量
		// Combiner的输出kv应该跟reducer输出的kv对应
		job.setCombinerClass(IntSumReducer.class);

		// 修改2：
		// 接收全局所有mapper的输出结果
		job.setReducerClass(IntSumReducer.class);

		// 输出类型map的K/V类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置reduce数量,默认1
		//job.setNumReduceTasks(1);

		// 添加输入路径
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		// 最后一个是输出地址路径
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[(otherArgs.length - 1)]));
		System.exit((job.waitForCompletion(true)) ? 0 : 1);

	}

	/**
	 * <Object, Text, Text, IntWritable> 1：文件偏移量，每个字或者词在文件中的下标,LongWritable也可以表示
	 * 2：输入数据类型 3/4输出的:key/value
	 * 
	 * @author Administrator
	 *
	 */
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		/**
		 * key:该行数据在文件中的下标 value:这行数据
		 */
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 获取一行数据
			String arrValues = new String(value.getBytes());
			// 切割，
			String[] split = arrValues.split("");
			for (String word : split) {
				context.write(new Text(word), new IntWritable(1));
			}

			/* 方式2
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreElements()) {
				context.write(new Text(st.nextToken()), new IntWritable(1));
			}*/
		}
	}

	/**
	 * <Text, IntWritable, Text, IntWritable> 指定输入输出 key/value的类型
	 * 
	 * @author Administrator
	 *
	 */
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * Iterable<IntWritable> values：迭代器 map所有结果，key的所有参数结果汇总
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable intWritable : values) {
				count += intWritable.get();
			}
			context.write(key, new IntWritable(count));

		}
	}
}
