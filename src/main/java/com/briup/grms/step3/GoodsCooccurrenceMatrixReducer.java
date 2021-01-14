package com.briup.grms.step3;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class GoodsCooccurrenceMatrixReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
			//20001:【20002,20005,20006,20005,20006】
		
		Map<String, Integer> map = new TreeMap<>();
		
		for(Text v : values){
			
			boolean containsKey = map.containsKey(v.toString());
			if(containsKey){//累加
				Integer integer = map.get(v.toString());
				map.put(v.toString(), integer+1);
			}else{//新加
				map.put(v.toString(), 1);
			}
		}
		
		StringBuffer sb = new StringBuffer();
		Set<Entry<String,Integer>> entrySet = map.entrySet();
		for(Entry<String,Integer> e: entrySet){
			sb.append(e.getKey()).append(":").append(e.getValue()).append(",");
		}
		sb.delete(sb.length()-1, sb.length());
		context.write(key, new Text(sb.toString()));
	}
}
