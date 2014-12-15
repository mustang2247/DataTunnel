package com.ganqiang.datatunnel.api.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.db.DBExecutor;

public class DBWriteExample implements Writeable{

	@SuppressWarnings({"unused", "unchecked"})
	@Override
	public void write(Param param) {
		Pair pair = param.getPair();
		DBExecutor dbexe = new DBExecutor(pair.getWriterPoolId());
		List<Map<String, Object>> readResults = (List<Map<String, Object>>) param.getReadResult();
		for (Map<String, Object> map : readResults) {
			String sql = "select * from project";
			List<String> sqllist = new ArrayList<String>();
			sqllist.add(sql);
			dbexe.executeBatch(sqllist);	
		}
	}

}
