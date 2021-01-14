package com.briup.grms.step5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 处理三的结果,拼接a:
 * */
public class Test5Mapper extends Mapper<Text, Text, Text, Text>{
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		context.write(key, new Text("a:"+value.toString()));
		
	}
}
