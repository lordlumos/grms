package com.briup.grms.step4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.javac.comp.Enter;

public class UserBuyGoodsVectorReducer extends Reducer<Text, Text, Text, Text>{
	/**
	 * 20001 : [10001,10002,10003]
	 * */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
		Map<String, Integer> map = new HashMap<>();
		for(Text t : values){
			String k1 = t.toString();
			boolean containsKey = map.containsKey(k1);
			if(containsKey){
				Integer integer = map.get(k1);
				
				map.put(k1, integer+1);
			}else{
				map.put(k1, 1);
			}
		}
		StringBuffer sb = new StringBuffer();
		Set<Entry<String,Integer>> entrySet = map.entrySet();
		for(Entry<String,Integer> e : entrySet){
			sb.append(e.getKey()).append(":").append(e.getValue()).append(",");
		}
		sb.delete(sb.length()-1, sb.length());
		context.write(key, new Text(sb.toString()));
	}
	/*
	 * 
	 		20001	10001:1,10004:1,10005:1
            20002	10001:1,10003:1,10004:1
            20003	10002:1
            20004	10002:1,10006:1
            20005	10001:1,10004:1
            20006	10001:1,10002:1,10004:1
            20007	10001:1,10003:1,10006:1
	 * 
	 * */
}
