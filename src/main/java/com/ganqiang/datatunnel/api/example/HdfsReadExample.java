package com.ganqiang.datatunnel.api.example;

import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.hdfs.HdfsExecutor;

public class HdfsReadExample implements Readable{

	@Override
	public Object read(Param param) {
		Pair pair = param.getPair();
		HdfsExecutor hbe = new HdfsExecutor(pair.getReaderPoolId());
		String str = hbe.read("/abc/123.txt");
		return str;
	}
		
}