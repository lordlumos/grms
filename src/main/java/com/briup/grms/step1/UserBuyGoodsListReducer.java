package com.briup.grms.step1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 * */
public class UserBuyGoodsListReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {

		ArrayList<String> list = new ArrayList<>();
		for(Text v : values){
			list.add(v.toString());
		}
		Collections.sort(list);
		
		StringBuffer sb = new StringBuffer();
		for(String v : list){
			sb.append(v);
			sb.append(",");
		}
		sb.delete(sb.length()-1, sb.length());
		context.write(key, new Text(sb.toString()));
	}

}
