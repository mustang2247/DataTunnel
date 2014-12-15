package com.ganqiang.datatunnel.conf;

import org.dom4j.Document;

public abstract class AbstractConfig {

	/******** xml node ************/
	protected static final String pool_node = "pool";
	protected static final String readers_node = "readers";	
	protected static final String writers_node = "writers";		
	protected static final String reader_node = "reader";	
	protected static final String writer_node = "writer";	
	protected static final String url_node = "url";
	protected static final String type_node = "type";
	protected static final String open_node = "open";
	protected static final String id_node = "id";
	protected static final String thread_num_node = "thread_num";
	protected static final String user_name_node = "user_name";
	protected static final String password_node = "password";
	protected static final String pool_size_node = "pool_size";
	protected static final String quorum_node = "quorum";
	protected static final String client_port_node = "client_port";
	
	
	protected static final String job_node = "job";
	protected static final String task_node = "task";
	protected static final String timer_node = "timer";
	protected static final String pool_id_node = "pool_id";
	protected static final String interval_node = "interval";
	protected static final String start_time_node = "start_time";
	protected static final String pair_node = "pair";
	

	abstract String getConfigFile();

	protected Document getDocument() {
		Document doc = XmlHelper.loadXML(getConfigFile());
		return doc;
	}

	abstract void loading();

}
