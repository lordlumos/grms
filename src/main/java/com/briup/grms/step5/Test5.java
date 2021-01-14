package com.briup.grms.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 商品共现矩阵乘以用户购买向量<Br>
   数据1：第三步的结果 商品的共现次数
	20001	{20001=3, 20002=2, 20005=2, 20006=2, 20007=1}
	20002	{20001=2, 20002=3, 20005=2, 20006=2, 20007=2}
	20003	{20003=1, 20004=1, 20006=1}
	20004	{20003=1, 20004=2, 20006=1, 20007=1}
	20005	{20001=2, 20002=2, 20005=2, 20006=2, 20007=1}
	20006	{20001=2, 20002=2, 20003=1, 20004=1, 20005=2, 20006=3, 20007=1}
	20007	{20001=1, 20002=2, 20004=1, 20005=1, 20006=1, 20007=3}
   数据2：第四步的结果  用户的购买向量
	20001	10001:1,10004:1,10005:1
	20002	10001:1,10004:1,10003:1
	20003	10002:1
	20004	10002:1,10006:1
	20005	10001:1,10004:1
	20006	10002:1,10001:1,10004:1
	20007	10001:1,10003:1,10006:1

 * 
 * */
public class Test5 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new Test5(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "step5");
		job.setJarByClass(this.getClass());
		//第三步的结果
		Path p1 = new Path("/rawdata/step3/01/part-r-00000");
		MultipleInputs.addInputPath(job, p1, KeyValueTextInputFormat.class,Test5Mapper.class);
		//第四步的结果
		Path p2 = new Path("/rawdata/step4/01/part-r-00000");
		MultipleInputs.addInputPath(job, p2, KeyValueTextInputFormat.class,Test5Mapper2.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(Test5Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/rawdata/step5/01"));

		job.waitForCompletion(true);
		return 0;
	}
}
