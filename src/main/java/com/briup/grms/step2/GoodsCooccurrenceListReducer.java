package com.briup.grms.step2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//得到共现关系
public class GoodsCooccurrenceListReducer extends Reducer<Text, NullWritable, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<NullWritable> value, Reducer<Text, NullWritable, Text, Text>.Context context)throws IOException, InterruptedException {
		String[] split = key.toString().split(",");
		for(int i = 0;i<split.length;i++){
			
			for(int j = 0 ;j<split.length;j++){
				context.write(new Text(split[i]), new Text(split[j]));
			}
		}
	}
}
