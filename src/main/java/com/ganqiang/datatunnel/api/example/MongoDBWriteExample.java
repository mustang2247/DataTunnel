package com.ganqiang.datatunnel.api.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.mongodb.MongoDBExecutor;
import com.mongodb.DBObject;

public class MongoDBWriteExample implements Writeable{

	@SuppressWarnings("unchecked")
	@Override
	public void write(Param param) {
		Pair pair = param.getPair();
		List<DBObject> list = (List<DBObject>)param.getReadResult();
		System.out.println("read list size: "+ list.size());
		MongoDBExecutor hbe = new MongoDBExecutor(pair.getWriterPoolId());
		List<Map<String,Object>> list2 = new ArrayList<Map<String,Object>>();
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("age", "aaa");
		map.put("name", "sdfasdfasdfas");
		list2.add(map);
		hbe.insert("tn", map);
	}

}