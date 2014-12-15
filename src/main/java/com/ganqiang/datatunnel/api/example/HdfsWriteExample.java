package com.ganqiang.datatunnel.api.example;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.hdfs.HdfsExecutor;

public class HdfsWriteExample  implements Writeable{

	@Override
	public void write(Param param) {
		Pair pair = param.getPair();
		HdfsExecutor hbe = new HdfsExecutor(pair.getWriterPoolId());
		hbe.writeLine("/abc/89.txt", "asdfasdfafdadf");
	}

}
