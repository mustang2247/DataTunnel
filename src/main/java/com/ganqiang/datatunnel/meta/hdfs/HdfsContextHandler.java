package com.ganqiang.datatunnel.meta.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.ganqiang.datatunnel.conf.Pool;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.meta.Visitable;
import com.ganqiang.datatunnel.meta.Visitor;

public class HdfsContextHandler implements Visitable {

	private Pool pool;

	public HdfsContextHandler(Pool pool){
		this.pool = pool;
	}

	public void init() {
		Configuration config = new Configuration();
		config.setBoolean("dfs.support.append", true);
		config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		config.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
		try {
			FileSystem hdfs = FileSystem.newInstance(URI.create(pool.getUrl()), config);
			Constants.hdfs_map.put(pool.getId(), hdfs);
		} catch (Exception e) {
		}
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visitHdfsContext(this);
	}

}
