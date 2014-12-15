package com.ganqiang.datatunnel.api.example;

import java.util.List;
import java.util.Map;

import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.hive.HiveExecutor;

public class HiveReadExample implements Readable{

	@Override
	public Object read(Param param) {
		Pair pair = param.getPair();
		HiveExecutor hbe = new HiveExecutor(pair.getReaderPoolId());
		List<Map<String, Object>> list = hbe.find(pair.getReaderValue());
		return list;
	}
		
}