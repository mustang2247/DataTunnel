package com.bytegriffin.datatunnel.meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;

public class HBaseContext implements Initializer {

	private static final Logger logger = LogManager.getLogger(HBaseContext.class);
	//注意cf与column之间用点号"."，不采用分号:分隔的是因为jsqlparser不支持带分号的sql语法解析
	public static final String column_familiy_split = ".";
	//hbase主键名称
	public static final String row_key = "row";

	@Override
	public void init(OperatorDefine operator) {
		String zkaddress = operator.getAddress();
		try {
			Configuration configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", zkaddress);
			Connection connection = ConnectionFactory.createConnection(configuration);
			if (connection.isClosed()) {
				logger.error("任务[{}]加载组件HBaseContext[{}]没有连接成功。", operator.getName(), operator.getId());
				System.exit(1);
			}
			Globals.setHBaseConnection(operator.getKey(), connection);
			logger.info("任务[{}]加载组件HBaseContext[{}]的初始化完成。", operator.getName(), operator.getId());
		} catch (Exception re) {
			logger.error("任务[{}]加载组件HBaseContext[{}]没有连接成功。", operator.getName(), operator.getId(), re);
		}
	}

}
