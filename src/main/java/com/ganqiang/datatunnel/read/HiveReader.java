package com.ganqiang.datatunnel.read;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.core.Process;
import com.ganqiang.datatunnel.meta.db.SqlFilter;
import com.ganqiang.datatunnel.meta.hive.HiveExecutor;
import com.ganqiang.datatunnel.util.ReflectionUtil;

public class HiveReader implements Process{

	private static final Logger logger = Logger.getLogger(HiveReader.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Param param) {
		logger.info("task "+Thread.currentThread().getName()+" start to read... ");
		Pair pair = param.getPair();
		HiveExecutor dbexe = new HiveExecutor(pair.getReaderPoolId());
		if(Constants.class_type.equals(pair.getReaderType())){
			Readable reader  = (Readable) ReflectionUtil.getInstance(pair.getReaderValue());
			List<Map<String, Object>> list = (List<Map<String, Object>>) reader.read(param);
			param.setReadResult(list);
		} else {
			String newsql = SqlFilter.replaceSelectSql(pair.getReaderValue());
			List<Map<String, Object>> list = dbexe.find(newsql);	
			param.setReadResult(list);
		}
		logger.info("task "+Thread.currentThread().getName()+" read finish.");
	}

}
