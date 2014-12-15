package com.ganqiang.datatunnel.read;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.api.Readable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.core.Process;
import com.ganqiang.datatunnel.util.ReflectionUtil;

public class HBaseReader implements Process{
	
	private static final Logger logger = Logger.getLogger(HBaseReader.class);

	@Override
	public void execute(Param param) {
		logger.info("task "+Thread.currentThread().getName()+" hbase start to read... ");
		Pair pair = param.getPair();
		if(Constants.class_type.equals(pair.getReaderType())){
			Readable reader  = (Readable) ReflectionUtil.getInstance(pair.getReaderValue());
			Object obj = reader.read(param);
			param.setReadResult(obj);
		}
		logger.info("task "+Thread.currentThread().getName()+" hbase read finish.");
	}

}
