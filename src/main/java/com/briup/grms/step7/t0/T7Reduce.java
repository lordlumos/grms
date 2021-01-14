package com.briup.grms.step7.t0;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 需要的结果:
  	10001	20003	1
	10001	20004	2
	10002	20001	2
	10002	20002	2
	10002	20005	2
	10002	20007	2
	10003	20001	3
	10003	20004	1
	10003	20005	3
	10003	20006	3
	10004	20003	1
	10004	20004	1
	10004	20007	5
	10005	20002	2
	10005	20005	2
	10005	20006	2
	10005	20007	1
	10006	20001	1
	10006	20002	2
	10006	20003	1
	10006	20005	1
	10006	20006	2

 * */
public class T7Reduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
		
		int i = 0;
		int max = 0;
		for(Text t : values){
			int v = Integer.parseInt(t.toString());
			if(v>max){
				max = v;
			}
			i++;
		}
		if(i>1){//相同
			return;
		}else{//不相同
			context.write(key, new Text(""+max));
		}
		
	
		
	}
}
















































































