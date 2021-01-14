package com.briup.grms.step7.t0;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 读取原始数据:
 * 		10001 20001 1
		10001 20002 1
		10001 20005 1
		10001 20006 1
		10001 20007 1
		10002 20003 1
		10002 20004 1
		10002 20006 1
		10003 20002 1
		10003 20007 1
		10004 20001 1
		10004 20002 1
		10004 20005 1
		10004 20006 1
		10005 20001 1
		10006 20004 1
		10006 20007 1
 * */
public class T7Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException, InterruptedException {
		String[] split = value.toString().split("[ ]");
		context.write(new Text(split[0]+","+split[1]),new Text("1"));
		System.out.println("--------map1输出--------------");
		System.out.println(split[0]+","+split[1]+"---"+1);
		System.out.println("--------map1输出--------------");
	}
}











































