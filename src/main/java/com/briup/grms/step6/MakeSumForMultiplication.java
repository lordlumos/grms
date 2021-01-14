package com.briup.grms.step6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用第五步结果的数据，进行key相同的求和
 * */
public class MakeSumForMultiplication extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new MakeSumForMultiplication(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "step");
		job.setJarByClass(this.getClass());

		job.setMapperClass(MakeSumForMultiplicationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MakeSumForMultiplicationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path("/rawdata/step5/01/part-r-00000"));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/rawdata/step6/01/"));

		job.waitForCompletion(true);
		
		return 0;
	}
}

