package com.briup.grms.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 步骤1的主类<Br>
 * 使用hdfs集群上的	matrix.txt文件数据进行计算，得到用户购买商品的列表
 *  	10001	20001,20005,20006,20007,20002
        10002	20006,20003,20004
        10003	20002,20007
        10004	20001,20002,20005,20006
        10005	20001
        10006	20004,20007
        
        yarn jar grms-1.jar com.briup.grms.step1.UserBuyGoodsList -D n=01
 * */
public class UserBuyGoodsList extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new UserBuyGoodsList(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		//第几次测试
		String n = "01";
		
		
		Job job = Job.getInstance(conf, "step1");
		job.setJarByClass(this.getClass());

		job.setMapperClass(UserBuyGoodsListMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(UserBuyGoodsListReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path("/rawdata/matrix.txt"));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/rawdata/step1/"+n));

		job.waitForCompletion(true);
		
		return 0;
	}
}
