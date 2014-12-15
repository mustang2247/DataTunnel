package com.ganqiang.datatunnel.api.example;

import java.util.List;
import java.util.Map;
import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.db.DBExecutor;

public class DBReadExample implements Readable{

	@Override
	public Object read(Param param) {
		Pair pair = param.getPair();
		DBExecutor dbexe = new DBExecutor(pair.getReaderPoolId());
		List<Map<String, Object>> list = dbexe.find("select * from project");	
		return list;
	}

}
