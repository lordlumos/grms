package com.briup.grms.step5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 处理结果4，拼接b:
 * */
public class Test5Mapper2 extends Mapper<Text, Text, Text, Text>{
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		context.write(key, new Text("b:"+value.toString()));
		
	}
}
