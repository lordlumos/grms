package com.briup.grms.step7.t0;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.briup.grms.step7.DuplicateDataForResultFirstMapper;
import com.briup.grms.step7.DuplicateDataForResultReducer;
import com.briup.grms.step7.DuplicateDataForResultSecondMapper;

public class T7 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new T7(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		// step7
		Job job = Job.getInstance(conf, "step7");
		job.setJarByClass(this.getClass());
		
		//原始数据
		Path in71 = new Path("/rawdata/matrix.txt");
		MultipleInputs.addInputPath(job, in71, TextInputFormat.class,T7Mapper1.class);
		//in72第六步的输出
		Path in72 = new Path("/rawdata/step6/01/part-r-00000");
		MultipleInputs.addInputPath(job, in72, TextInputFormat.class,T7Mapper2.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(T7Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job,new Path("/rawdata/step7/02/"));
		
		job.waitForCompletion(true);
		return 0;
	}
}
