package com.ganqiang.datatunnel.meta.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.ganqiang.datatunnel.conf.Pool;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.meta.Visitable;
import com.ganqiang.datatunnel.meta.Visitor;

public class HBaseContextHandler implements Visitable {

	private Pool pool;
	
	public HBaseContextHandler(Pool pool){
		this.pool = pool;
	}

	public void init() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", pool.getQuorum());
		conf.set("hbase.zookeeper.property.clientPort", pool.getClientPort());
		HBaseConnController cc = new HBaseConnController();
		cc.setConfig(conf);
		Constants.hbase_conn_map.put(pool.getId(), cc);
	}


	@Override
	public void accept(Visitor visitor) {
		visitor.visitHBaseContext(this);
	}

}
