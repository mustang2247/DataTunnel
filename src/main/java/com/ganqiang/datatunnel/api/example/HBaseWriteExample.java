package com.ganqiang.datatunnel.api.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.ganqiang.datatunnel.api.Writeable;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Param;
import com.ganqiang.datatunnel.meta.hbase.HBaseExecutor;

public class HBaseWriteExample implements Writeable{

	@Override
	public void write(Param param) {
		Pair pair = param.getPair();
		HBaseExecutor hbe = new HBaseExecutor(pair.getWriterPoolId());
		List<Put> list2 = new ArrayList<Put>();
		for (int i=0; i<2; i++) {
			Put p = new Put(Bytes.toBytes("123"));
			p.add(Bytes.toBytes("abc"), Bytes.toBytes("NAME"), Bytes.toBytes("aaaaa"));
			p.add(Bytes.toBytes("abc"),Bytes.toBytes("BORROWER"),Bytes.toBytes("bbbb"));
			list2.add(p);
		}
		hbe.putBatch("test1", list2);
	}

}