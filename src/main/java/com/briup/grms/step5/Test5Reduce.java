package com.briup.grms.step5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Test5Reduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		System.err.println("--------------------------");
		System.out.println("key:"+key);
		//步骤3结果的部分值
		String a3end = null;
		//步骤4结果的部分值
		String b4end = null;
		for(Text v : values){
			String string = v.toString();
			if(string.indexOf("a:")!=-1){
				a3end = string.replace("a:", "");
			}else if(string.indexOf("b:")!=-1){
				b4end = string.replace("b:", "");
			}
		}
		System.out.println("a:"+a3end);//a:20001:2,20002:3,20005:2,20006:2,20007:2
		System.out.println("b:"+b4end);//b:10001:1,10004:1,10003:1
		
		System.out.println("--------------------------------");
		String[] as = a3end.split(",");
		String[] bs = b4end.split(",");
		for(String b:bs){
			String[] b1 = b.split(":");
			for(String a:as){
				String[] a1 = a.split(":");
				int v = Integer.parseInt(b1[1])*Integer.parseInt(a1[1]);
				context.write(new Text(b1[0]+","+a1[0]), new Text(""+v));
			}
		}
	}
	
/*	
  
  key:
		20007
   value:

		b:10001:1,10003:1,10006:1
		a:20001:1,20002:2,20004:1,20005:1,20006:1,20007:3
*/
}
