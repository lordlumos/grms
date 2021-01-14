package com.briup.grms.step4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.briup.grms.utils.JobUtil;

/**
 * 使用第一步的结果计算用户的购买向量:
 * 	结果：
 * 	 		20001	10001:1,10004:1,10005:1
            20002	10001:1,10003:1,10004:1
            20003	10002:1
            20004	10002:1,10006:1
            20005	10001:1,10004:1
            20006	10001:1,10002:1,10004:1
            20007	10001:1,10003:1,10006:1
 * */
public class UserBuyGoodsVector extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new UserBuyGoodsVector(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//第几次测试
		String n = "01";
		JobUtil.setConf(conf, this.getClass(), "step4", "/rawdata/step1/01/part-r-00000", "/rawdata/step4/"+n);
		JobUtil.setMapper(UserBuyGoodsVectorMapper.class, Text.class, Text.class, TextInputFormat.class);
		JobUtil.setReducer(UserBuyGoodsVectorReducer.class, Text.class, Text.class, TextOutputFormat.class, 1);
		JobUtil.commit();
		return 0;
	}
}





























