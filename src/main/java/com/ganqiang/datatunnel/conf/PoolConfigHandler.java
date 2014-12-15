package com.ganqiang.datatunnel.conf;

import java.util.List;

import org.dom4j.Element;
import org.dom4j.Node;

import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.core.DataPoolType;
import com.ganqiang.datatunnel.meta.Visitable;
import com.ganqiang.datatunnel.meta.Visitor;

public class PoolConfigHandler extends AbstractConfig implements Visitable{

	private final String pool_xml = System.getProperty("user.dir")	+ "/conf/datatunnel-pool.xml";

	@Override
	String getConfigFile() {
		return pool_xml;
	}

	@SuppressWarnings({"unchecked", "incomplete-switch"})
	@Override
	public void loading() {
		Element poolNode = XmlHelper.getElement(getDocument(), pool_node);
		Node readersNode = poolNode.selectSingleNode(readers_node);
		List<Element> readerList = readersNode.selectNodes(reader_node);
		for (Element reader : readerList) {
			Pool pool = new Pool();
			String idvalue = reader.elementText(id_node);
			String typevalue = reader.elementText(type_node);
			String open = reader.elementText(open_node);
			pool.setId(idvalue);
			pool.setType(typevalue);
			pool.setOpen(Boolean.valueOf(open));
			DataPoolType dptype = DataPoolType.valueOf(typevalue);
			switch(dptype){
				case MysqlReader:
				case OracleReader:
				case HiveReader:
				case MongoDBReader:	
					String username = reader.elementText(user_name_node);
					String password = reader.elementText(password_node);
					String poolsize = reader.elementText(pool_size_node);
					String url = reader.elementText(url_node);
					pool.setUrl(url);
					pool.setUserName(username);
					pool.setPassword(password);
					pool.setPoolSize(Integer.valueOf(poolsize));
					break;
				case HBaseReader:
					String quorum = reader.elementText(quorum_node);
					String clientport = reader.elementText(client_port_node);
					pool.setQuorum(quorum);
					pool.setClientPort(clientport);
					break;
				case HdfsReader:
					String url2 = reader.elementText(url_node);
					pool.setUrl(url2);
					break;
				case RedisReader:
					String url3 = reader.elementText(url_node);
					pool.setUrl(url3);
					break;
			}
			Constants.conf_pool_map.put(idvalue, pool);
		}

		Node writersNode = poolNode.selectSingleNode(writers_node);
		List<Element> writerList = writersNode.selectNodes(writer_node);
		for (Element writer : writerList) {
			Pool pool = new Pool();
			String idvalue = writer.elementText(id_node);
			String typevalue = writer.elementText(type_node);
			String open = writer.elementText(open_node);
			pool.setId(idvalue);
			pool.setType(typevalue);
			pool.setOpen(Boolean.valueOf(open));
			DataPoolType dptype = DataPoolType.valueOf(typevalue);
			switch(dptype){
				case MysqlWriter:
				case OracleWriter:
				case HiveWriter:
				case MongoDBWriter:
					String username = writer.elementText(user_name_node);
					String password = writer.elementText(password_node);
					String poolsize = writer.elementText(pool_size_node);
					String url = writer.elementText(url_node);
					pool.setUrl(url);
					pool.setUserName(username);
					pool.setPassword(password);
					pool.setPoolSize(Integer.valueOf(poolsize));
					break;
				case HBaseWriter:
					String quorum = writer.elementText(quorum_node);
					String clientport = writer.elementText(client_port_node);
					pool.setQuorum(quorum);
					pool.setClientPort(clientport);
					break;
				case HdfsWriter:
					String url2 = writer.elementText(url_node);
					pool.setUrl(url2);
					break;
				case RedisWriter:
					String url3 = writer.elementText(url_node);
					pool.setUrl(url3);
					break;
			}
			Constants.conf_pool_map.put(idvalue, pool);
		}
	}

	public static void main(String... args) {
		PoolConfigHandler conf = new PoolConfigHandler();
		conf.loading();
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visitPoolConfig(this);
	}

}
