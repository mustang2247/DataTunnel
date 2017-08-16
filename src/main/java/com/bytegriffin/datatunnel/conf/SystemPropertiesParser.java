package com.bytegriffin.datatunnel.conf;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.core.Globals;

public class SystemPropertiesParser {

	private static final Logger logger = LogManager.getLogger(SystemPropertiesParser.class);
	private static String system_properties = TasksDefineXmlParser.conf_path + "system.properties";
	private static final String redis_mode = "redis.mode";
	private static final String redis_data_type = "redis.data.type";

	public void load(){
		try {
			PropertiesConfiguration conf = new PropertiesConfiguration(new File(system_properties));
			Globals.redis_mode = conf.getString(redis_mode);
			Globals.redis_data_type = conf.getString(redis_data_type);
			
			logger.info("配置文件[{}]读取完成。", system_properties);
		} catch (ConfigurationException e) {
			logger.error("配置文件[{}]读取失败。", system_properties);
		}
	}

}
