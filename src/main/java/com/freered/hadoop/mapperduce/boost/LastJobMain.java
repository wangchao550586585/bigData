package com.freered.hadoop.mapperduce.boost;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastJobMain {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("yarn.resourcemanager.hostname", "hadoop01");
			Job job = Job.getInstance(conf, "weibo3");

			job.setJarByClass(LastJobMain.class);

			// 添加2个缓存文件到job中
			// 文件内容为N
			job.addCacheFile(new Path("/sanbox/output01/part-r-0002").toUri());
			// 文件内容为DF
			job.addCacheFile(new Path("/sanbox/output02/part-r-0000").toUri());

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(LastMapper.class);
			job.setReducerClass(LastReduce.class);

			// 读取TF
			FileInputFormat.addInputPath(job, new Path("/sanbox/output01"));

			FileOutputFormat.setOutputPath(job, new Path("/sanbox/output03"));
			System.exit((job.waitForCompletion(true)) ? 0 : 1);
		} catch (Exception e) {
		}
	}

	static class LastMapper extends Mapper<LongWritable, Text, Text, Text> {
		public static Map<String, Integer> cmap = null;
		public static Map<String, Integer> df = null;

		// 在map方法之前执行，获取缓冲文件，内容存储在cmap和df中
		protected void setup(Context context) throws IOException,
				InterruptedException {
			if (cmap == null || cmap.size() == 0 || df == null
					|| df.size() == 0) {
				URI[] ss = context.getCacheFiles();
				if (ss != null) {
					for (int i = 0; i < ss.length; i++) {
						URI uri = ss[i];
						if (uri.getPath().endsWith("part-r-00002")) {
							Path path = new Path(uri.getPath());
							BufferedReader br = new BufferedReader(
									new FileReader(path.getName()));
							String line = br.readLine();
							if (line.startsWith("count")) {
								String[] ls = line.split("\t");
								cmap = new HashMap<String, Integer>();
								cmap.put(ls[0], Integer.parseInt(ls[1].trim()));
							}
							br.close();
						} else if (uri.getPath().endsWith("part-r-0000")) {
							df = new HashMap<String, Integer>();
							Path path = new Path(uri.getPath());
							BufferedReader br = new BufferedReader(
									new FileReader(path.getName()));
							String line = null;
							if ((line = br.readLine()) != null) {
								String[] ls = line.split("\t");
								df.put(ls[0], Integer.parseInt(ls[1].trim()));
							}
							br.close();
						}
					}
				}
			}
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			FileSplit fs = (FileSplit) context.getInputSplit();
			// part-r-0002数据为微博总数
			if (!fs.getPath().getName().contains("part-r-0002")) {
				String[] v = value.toString().trim().split("\t");
				if (v.length >= 2) {
					int tf = Integer.parseInt(v[1].trim());
					String[] ss = v[0].split("_");
					if (ss.length >= 2) {
						String word = ss[0];
						String wordId = ss[1];
						double s = tf
								* Math.log(cmap.get("count") / df.get(word));

						// 格式化，保留小数点最后5位
						NumberFormat instance = NumberFormat.getInstance();
						instance.setMaximumFractionDigits(5);
						context.write(new Text(wordId), new Text(word + ":"
								+ instance.format(s)));
					}
				}
			}
		}
	}

	static class LastReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text i : iterable) {
				sb.append(i.toString() + "\t");
			}
			context.write(key, new Text(sb.toString()));
		}

	}

}
