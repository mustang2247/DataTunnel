package com.ganqiang.datatunnel.write;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.core.Process;
import com.ganqiang.datatunnel.util.ReflectionUtil;

public class HBaseWriter implements Process{

	private static final Logger logger = Logger.getLogger(HBaseWriter.class);
	
	@Override
	public void execute(Param param) {
		logger.info("task "+Thread.currentThread().getName()+" hbase start to write... ");
		Pair pair = param.getPair();
		if(Constants.class_type.equals(pair.getWriterType())){
			Writeable writer  = (Writeable) ReflectionUtil.getInstance(pair.getWriterValue());		
			writer.write(param);
		}		
		logger.info("task "+Thread.currentThread().getName()+" hbase write finish.");
	}

}
