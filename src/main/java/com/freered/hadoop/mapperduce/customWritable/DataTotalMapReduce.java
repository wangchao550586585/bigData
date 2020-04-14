package com.freered.hadoop.mapperduce.customWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DataTotalMapReduce {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(DataTotalMapReduce.class);
        job.setMapperClass(DataTotalMapper.class);
        job.setReducerClass(DataTotalReducer.class);
        job.setCombinerClass(DataTotalReducer.class);
        //指定最终输出的Key
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataWritable.class);
        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[(args.length - 1)]));

        System.exit((job.waitForCompletion(true)) ? 0 : 1);
    }
}

/**
 * 1363157985066 13726230503 00-FD-07-A4-72-B8:CMCC 120.196.100.82
 * i02.c.aliimg.com 24 27 2481 24681 200 1363157995052 13826544101
 * 5C-0E-8B-C7-F1-E0:CMCC 120.197.40.4 4 0 264 0 200 1363157991076 13926435656
 * 20-10-7A-28-CC-0A:CMCC 120.196.100.99 2 4 132 1512 200
 */
class DataTotalMapper extends Mapper<LongWritable, Text, Text, DataWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String lineStr = value.toString();
        String[] strArr = lineStr.split("\t");
        String phpone = strArr[1];
        String upPackNum = strArr[6];
        String downPackNum = strArr[7];
        String upPayLoad = strArr[8];
        String downPayLoad = strArr[9];
        context.write(
                new Text(phpone),
                new DataWritable(Integer.parseInt(upPackNum), Integer
                        .parseInt(downPackNum), Integer.parseInt(upPayLoad),
                        Integer.parseInt(downPayLoad)));
    }
}

class DataTotalReducer extends Reducer<Text, DataWritable, Text, DataWritable> {
    @Override
    protected void reduce(Text k2, Iterable<DataWritable> v2, Context context)
            throws IOException, InterruptedException {
        int upPackNumSum = 0;
        int downPackNumSum = 0;
        int upPayLoadSum = 0;
        int downPayLoadSum = 0;
        for (DataWritable dataWritable : v2) {
            upPackNumSum += dataWritable.getUpPackNum();
            downPackNumSum += dataWritable.getDownPackNum();
            upPayLoadSum += dataWritable.getUpPayLoad();
            downPayLoadSum += dataWritable.getDownPayLoad();
        }
        context.write(k2, new DataWritable(upPackNumSum, downPackNumSum, upPayLoadSum, downPayLoadSum));
    }
}