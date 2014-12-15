package com.ganqiang.datatunnel;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.ganqiang.datatunnel.core.JobScheduler;
import com.ganqiang.datatunnel.meta.Prepare;
import com.ganqiang.datatunnel.meta.Visitor;

public class DataTunnel {

	private static final Logger logger = Logger.getLogger(DataTunnel.class);
	private static final String log4j = System.getProperty("user.dir")
			+ "/conf/log4j.conf";

	static {
		PropertyConfigurator.configure(log4j);
		logger.info("Loading log4j configuration file...");
	}

	public void startup(){
		Visitor visitor = new Prepare();
		visitor.visitAll();
		JobScheduler.run();
	}

	public static void main(String... args){
			logger.info("Start the DataTunnel......");
			DataTunnel dt = new DataTunnel();
			dt.startup();
	}

}
