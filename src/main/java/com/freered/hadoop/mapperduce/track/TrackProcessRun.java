package com.freered.hadoop.mapperduce.track;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 用户日常轨迹分析
 * 
 * 汇总基站数据表 计算每个月用户在不同时间段不同的基站停留时长 3个时间段统计:9-17,17-24,24-9
 * hadoop jar track.jar com.freered.hadoop.mapperduce.track.TrackProcessRun
 *
 */
public class TrackProcessRun extends Configured implements Tool {
	enum Counter {
		TIMESKIP, // 时间格式有误
		OUTOFTIMESKIP;// 时间不在参数指定时间内
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("fs.default.name", "hdfs://hadoop01:9000");
		conf.set("mapred.job.tracker", "hadoop01:9001");
		// conf.set("mapred.jar", "");

		// 统计某一天
		conf.set("date", "2016-01-29");
		// 统计时间段
		conf.set("timeout", "09-17-24");

		Job job = Job.getInstance(conf, "BaseStationDataPreprocess");
		job.setJarByClass(TrackProcessRun.class);

		FileInputFormat.addInputPath(job, new Path(""));
		FileOutputFormat.setOutputPath(job, new Path(""));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		String date;
		String[] timepoint;// 时间段
		boolean dataSource;// 是否位置数据还是网络数据

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.date = context.getConfiguration().get("date");
			this.timepoint = context.getConfiguration().get("timeout")
					.split("-");

			FileSplit fs = (FileSplit) context.getInputSplit();
			String filename = fs.getPath().getName();
			if (filename.startsWith("pos"))// 位置数据
				dataSource = true;
			else if (filename.startsWith("net"))// 网络通信数据
				dataSource = false;
			else
				throw new IOException("File Name should starts with POS or NET");
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			LineParser tableLine = new LineParser();

			try {
				tableLine.set(line, this.dataSource, this.date, this.timepoint);
			} catch (Exception e) {
				context.getCounter(Counter.TIMESKIP).increment(1);
				return;
			}
			context.write(tableLine.outKey(), tableLine.outValue());
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
		private String date;
		private SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.date = context.getConfiguration().get("date");
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] keys = key.toString().split("\\|");
			String imsi = keys[0];
			String timeFlag = keys[1];

			// 记录时间，treeMap自动排序了
			TreeMap<Long, String> uploads = new TreeMap<Long, String>();
			String[] split;
			for (Text value : values) {
				split = value.toString().split("\\|");
				try {
					uploads.put(Long.parseLong(split[1]), split[0]);
				} catch (NumberFormatException e) {
					context.getCounter(Counter.TIMESKIP).increment(1);
					continue;
				}
			}
			try {
				// 在最后位置添加OFF
				uploads.put(
						this.format.parse(
								this.date + " " + timeFlag.split("-")[1]
										+ ":00:00").getTime() / 1000L, "OFF");
				// 汇总数据
				HashMap<String, Float> locs = getStayTime(uploads);

				for (Entry<String, Float> entrySet : locs.entrySet()) {
					StringBuilder builder = new StringBuilder();
					builder.append(imsi).append("|");
					builder.append(entrySet.getKey()).append("|");
					builder.append(timeFlag).append("|");
					builder.append(entrySet.getValue());
					context.write(NullWritable.get(),
							new Text(builder.toString()));
				}
			} catch (ParseException e) {
				context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
				return;
			}

		}

		/**
		 * 获得位置停留信息
		 * 
		 * @param uploads
		 * @return
		 */
		private HashMap<String, Float> getStayTime(TreeMap<Long, String> uploads) {
			HashMap<String, Float> locs = new HashMap<String, Float>();
			Iterator<Entry<Long, String>> it = uploads.entrySet().iterator();
			Entry<Long, String> upload = it.next();
			Entry<Long, String> nextUpload;
			while (it.hasNext()) {
				nextUpload = it.next();
				float diff = (nextUpload.getKey() - upload.getKey()) / 60.0f;
				// 时间间隔超过60表示关机
				if (diff <= 60.0) {
					if (locs.containsKey(upload.getValue())) {
						locs.put(upload.getValue(), locs.get(upload.getValue())
								+ diff);
					} else {
						locs.put(upload.getValue(), diff);
					}
				}
				upload = nextUpload;
			}
			return locs;
		}

	}

	public static class LineParser {
		String imsi, position, time, timeFlag;
		Date date;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		public void set(String line, boolean dataSource, String date,
				String[] timepoint) throws Exception {
			String[] lineSplit = line.split("\t");
			// 根据数据类型获取用户识别码，位置，时间
			if (dataSource) {
				this.imsi = lineSplit[0];
				this.position = lineSplit[3];
				this.time = lineSplit[4];
			} else {
				this.imsi = lineSplit[0];
				this.position = lineSplit[2];
				this.time = lineSplit[3];
			}

			// 校验日期，统计某一天的日期要与数据文件中的日期一致
			if (this.time.startsWith(date))
				throw new Exception("-1");

			// 计算所属时间段
			int hour = Integer.valueOf(this.time.split(" ")[1].split(":")[0]);
			int i = 0, n = timepoint.length;
			while (i < n && Integer.valueOf(timepoint[i]) <= hour)
				i++;
			if (i < n) {
				if (i == 0) {
					this.timeFlag = "00-" + timepoint[i];
				} else {
					this.timeFlag = timepoint[i - 1] + "-" + timepoint[i];
				}
			} else {
				throw new Exception("-2");
			}
		}

		public Text outKey() {
			return new Text(this.imsi + "|" + this.timeFlag);
		}

		public Text outValue() {
			return new Text(this.position + "|" + date.getTime() / 1000L);
		}
	}
}
