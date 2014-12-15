package com.ganqiang.datatunnel.write;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.core.Process;

import com.ganqiang.datatunnel.meta.db.SqlFilter;
import com.ganqiang.datatunnel.meta.hive.HiveExecutor;
import com.ganqiang.datatunnel.util.ReflectionUtil;

public class HiveWriter implements Process{

	private static final Logger logger = Logger.getLogger(HiveWriter.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Param param) {
		logger.info("task "+Thread.currentThread().getName()+" start to write... ");
		Pair pair = param.getPair();
		HiveExecutor dbexe = new HiveExecutor(pair.getWriterPoolId());
		if(Constants.class_type.equals(pair.getWriterType())){
			Writeable writer  = (Writeable) ReflectionUtil.getInstance(pair.getWriterValue());		
			writer.write(param);
		} else {
			List<Map<String,Object>> pairlist = (List<Map<String, Object>>) param.getReadResult();
			List<String> hqls = SqlFilter.replaceUpdateSql(pair.getWriterValue(), pairlist);
			for (String hql: hqls){
				dbexe.execute(hql);
			}
			
		}
		logger.info("task "+Thread.currentThread().getName()+" write finish.");
	}

}
