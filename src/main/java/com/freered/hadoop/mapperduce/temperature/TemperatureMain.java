package com.freered.hadoop.mapperduce.temperature;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 还可以在这里配置文件输入输出目录，
 * 只需要输入 hadoop jar hadoop01-0.0.1-SNAPSHOT-jar-with-dependencies.jar Main所在类名也可运行
 * 
 * 计算1949-1955年，每年温度最高的时间
 * 计算1949-1955年，每年温度最高前10天
 * 思路：
 * 		按照年份升序排序，每一年的温度降序排序
 * 		按照年份分组，每一年对应一个reduce任务
 * 实现需要
 * 		自定排序
 * 		自定义分区
 * 		自定义分组
 * 		自定义Key
 * 测试数据
1949-10-01 14:21:02	34°C
1949-10-02 14:01:02	36°C
1950-01-01 11:21:02	32°C
1950-10-01 12:21:02	37°C
1951-12-01 12:21:02	23°C
1950-10-02 12:21:02	41°C
1950-10-03 12:21:02	27°C
1951-07-01 12:21:02	45°C
1951-07-02 12:21:02	46°C
 * @author Administrator
 *
 */
public class TemperatureMain {
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "temperature");
		job.setJarByClass(TemperatureMain.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReduce.class);
		job.setMapOutputKeyClass(KeyPair.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(3);
		job.setPartitionerClass(FirstPartition.class);
		job.setSortComparatorClass(SortTemperature.class);
		job.setGroupingComparatorClass(GroupTemperature.class);

		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[(otherArgs.length - 1)]));
		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}

	private static SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	static class TemperatureMapper extends
			Mapper<LongWritable, Text, KeyPair, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] val = value.toString().split("\t");
			try {
				Date date = sdf.parse(val[0]);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date);
				int year = calendar.get(1);
				String temperature = val[1].substring(0, val[1].indexOf("°C"));
				KeyPair keyPair = new KeyPair(year,
						Integer.parseInt(temperature));
				context.write(keyPair, value);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	static class TemperatureReduce extends
			Reducer<KeyPair, Text, KeyPair, Text> {
		protected void reduce(KeyPair key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}

		}
	}
}
