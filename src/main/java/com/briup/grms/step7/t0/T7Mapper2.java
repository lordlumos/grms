package com.briup.grms.step7.t0;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 读取第六步的结果<br>
 * 	10001,20001	10
	10001,20002	11
	10001,20003	1
	10001,20004	2
	10001,20005	9
	10001,20006	10
	10001,20007	8
	10002,20001	2
	10002,20002	2
	10002,20003	3
	10002,20004	4
	10002,20005	2
	10002,20006	5
	10002,20007	2
	10003,20001	3
	10003,20002	5
	10003,20004	1
	10003,20005	3
	10003,20006	3
	10003,20007	5
	10004,20001	9
	10004,20002	9
	10004,20003	1
	10004,20004	1
	10004,20005	8
	10004,20006	9
	10004,20007	5
	10005,20001	3
	10005,20002	2
	10005,20005	2
	10005,20006	2
	10005,20007	1
	10006,20001	1
	10006,20002	2
	10006,20003	1
	10006,20004	3
	10006,20005	1
	10006,20006	2
	10006,20007	4

 * */
public class T7Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
			
		System.out.println(value);
		
		String[] split = value.toString().split("\t");
		System.out.println(Arrays.toString(split));
		context.write(new Text(split[0]), new Text(split[1]));
		System.out.println("--------map2输出--------------");
		System.out.println(split[0]+"---"+split[1]);
		System.out.println("--------map2输出--------------");
		
	};

}








































