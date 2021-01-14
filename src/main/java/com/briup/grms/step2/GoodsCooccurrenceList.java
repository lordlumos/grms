package com.briup.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用第一步的结果，计算共现关系<br>
 * 数据:
		10001	20001,20002,20005,20006,20007
		10002	20003,20004,20006
		10003	20002,20007
		10004	20001,20002,20005,20006
		10005	20001
		10006	20004,20007

	预计结果:
	 		20001 20002
	 		20001 20005
	 		20001 20006
	 		20001 20007
	 		20002 20005
	 		20002 20006
	 		20002 20007
	 		20005 20006
	 		20005 20007
	 		20006 20007
	 		....
	 		...
	 		..
	 		.
	 		
		yarn jar grms-1.jar com.briup.grms.step2.GoodsCooccurrenceList -D n=01
 * */
public class GoodsCooccurrenceList extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new GoodsCooccurrenceList(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//第几次测试
		String n = "01";
		
		Job job = Job.getInstance(conf, "stap2");
		job.setJarByClass(this.getClass());
		

		job.setMapperClass(GoodsCooccurrenceListMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(GoodsCooccurrenceListReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path("/rawdata/step1/01/part-r-00000"));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/rawdata/step2/"+n));

		job.waitForCompletion(true);
		return 0;
	}
}
