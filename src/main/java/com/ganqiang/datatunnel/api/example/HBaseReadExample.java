package com.ganqiang.datatunnel.api.example;

import java.util.List;
import java.util.Map;

import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.hbase.HBaseExecutor;

public class HBaseReadExample implements Readable{

	@Override
	public Object read(Param param) {
		Pair pair = param.getPair();
		HBaseExecutor hbe = new HBaseExecutor(pair.getReaderPoolId());
		List<Map<String, String>> list = hbe.find("test1");
		return list;
	}
		
}
