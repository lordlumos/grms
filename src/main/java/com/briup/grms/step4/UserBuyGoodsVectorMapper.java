package com.briup.grms.step4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  10001	20001,20002,20005,20006,20007
	10002	20003,20004,20006
	10003	20002,20007
	10004	20001,20002,20005,20006
	10005	20001
	10006	20004,20007
 * */
public class UserBuyGoodsVectorMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException, InterruptedException {
		
		String[] split = value.toString().split("\t");
		
		String k = split[0];
		String[] vs =  split[1].split(",");
		
		for(String  v : vs){
			context.write(new Text(v), new Text(k));
		}
	}
}
