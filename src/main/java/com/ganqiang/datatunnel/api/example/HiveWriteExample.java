package com.ganqiang.datatunnel.api.example;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.hive.HiveExecutor;

public class HiveWriteExample implements Writeable{

	@Override
	public void write(Param param) {
		Pair pair = param.getPair();
		HiveExecutor hiveexe = new HiveExecutor(pair.getWriterPoolId());
		hiveexe.execute("insert into test(key) values('1')");
	}

}
