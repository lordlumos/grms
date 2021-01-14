package com.briup.grms.step6;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * key相同的求和
 * 			10001,20001	2
			10001,20001	2
			10001,20001	3
			10001,20001	1
			10001,20001	2
			10001,20002	3
			10001,20002	2
			10001,20002	2
			10001,20002	2
			10001,20002	2
			10001,20003	1
			10001,20004	1
			10001,20004	1
			10001,20005	2
			10001,20005	2
 * 
 * */
public class MakeSumForMultiplicationMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])));
	}
}
