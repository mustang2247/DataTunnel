package com.bytegriffin.datatunnel.meta;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.sql.SqlMapper;

public class LuceneContext implements Initializer{

	private static final Logger logger = LogManager.getLogger(LuceneContext.class);

	@Override
	public void init(OperatorDefine operator) {
		String sql = operator.getValue();
		String dir = operator.getAddress();
		if(dir.endsWith(File.separator)) {	
    		dir = dir + SqlMapper.getTableName(sql);
    	} else {
    		dir = dir + File.separator + SqlMapper.getTableName(sql);
    	}
    	try {
			Files.createDirectories(Paths.get(dir));
			FSDirectory fsdir = FSDirectory.open(Paths.get(dir));
			Globals.setLuceneDir(operator.getKey(), fsdir);
			logger.info("任务[{}]加载组件LuceneContext[{}]的初始化完成。", operator.getName(), operator.getId());
		} catch (IOException e) {
			logger.error("任务[{}]加载组件LuceneContext[{}]初始化失败。", operator.getName(), operator.getId(), e);
		}
	}

}
