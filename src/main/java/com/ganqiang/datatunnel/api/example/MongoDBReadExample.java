package com.ganqiang.datatunnel.api.example;

import java.util.List;
import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.mongodb.MongoDBExecutor;
import com.mongodb.DBObject;

public class MongoDBReadExample implements Readable{

	@Override
	public Object read(Param param) {
		Pair pair = param.getPair();
		MongoDBExecutor hbe = new MongoDBExecutor(pair.getReaderPoolId());
		List<DBObject> list = hbe.find("tn");
		return list;
	}

}