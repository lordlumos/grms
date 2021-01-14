package com.briup.grms.step3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 商品和商品的相似度
 * 使用步骤二的结果计算商品和商品的共现次数<br>
 * 原始数据:	
 *  20001	20002
	20001	20005
	20001	20006
	20002	20005
	20002	20006
	20005	20006
	20001	20002
	20001	20005
	20001	20006
	20001	20007
	20002	20005
	20002	20006
	20002	20007
	20005	20006
	20005	20007
	20006	20007
	20002	20007
	20003	20004
	20003	20006
	20004	20006
	20004	20007
	得到结果:
	

 * */
public class GoodsCooccurrenceMatrix extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new GoodsCooccurrenceMatrix(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		//第几次测试
		String n = "01";
		
		Job job = Job.getInstance(conf, "step3");
		job.setJarByClass(this.getClass());

		job.setMapperClass(GoodsCooccurrenceMatrixMappper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path("/rawdata/step2/01/part-r-00000"));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/rawdata/step3/"+n));

		job.waitForCompletion(true);
		return 0;
	}
}
