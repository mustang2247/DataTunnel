package com.ganqiang.datatunnel.core;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.conf.Task;
import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.read.DBReader;
import com.ganqiang.datatunnel.read.HBaseReader;
import com.ganqiang.datatunnel.read.HdfsReader;
import com.ganqiang.datatunnel.read.HiveReader;
import com.ganqiang.datatunnel.read.MongoDBReader;
import com.ganqiang.datatunnel.read.RedisReader;
import com.ganqiang.datatunnel.write.DBWriter;
import com.ganqiang.datatunnel.write.HBaseWriter;
import com.ganqiang.datatunnel.write.HdfsWriter;
import com.ganqiang.datatunnel.write.HiveWriter;
import com.ganqiang.datatunnel.write.MongoDBWriter;
import com.ganqiang.datatunnel.write.RedisWriter;

public final class JobScheduler {
	
	private static final Logger logger= Logger.getLogger(JobScheduler.class);

	public static void run() {
		List<Task> job = Constants.conf_job_list;
		for (Task task : job) {
			Long interval = task.getInterval();
			Date starttime = task.getStartTime();
			TaskController jc = new TaskController(task);
			List<Pair> pairlist = task.getPairs();
			for(Pair pair : pairlist){
				setting(pair);
			}
			logger.info("Task "+Thread.currentThread().getName()+" is ready to start...");
			if (interval == null && starttime == null) {
				jc.run();
			} else if (interval == null || interval == 0) {		
				Constants.timer.schedule(jc, starttime);
			} else {
				Constants.timer.schedule(jc, starttime, interval);
			}
			
		}
	}

	private static void setting(Pair pair){
		Chain chain = new LifeCycle();
		String rtype = Constants.conf_pool_map.get(pair.getReaderPoolId()).getType();
		String wtype = Constants.conf_pool_map.get(pair.getWriterPoolId()).getType();
		if (rtype.equalsIgnoreCase("MysqlReader") || wtype.equalsIgnoreCase("OracleReader")) {
			chain.addProcess(new DBReader());
		} else if(rtype.equalsIgnoreCase("HBaseReader")){
			chain.addProcess(new HBaseReader());
		} else if(rtype.equalsIgnoreCase("HdfsReader")){
			chain.addProcess(new HdfsReader());
		} else if(rtype.equalsIgnoreCase("HiveReader")){
			chain.addProcess(new HiveReader());
		} else if(rtype.equalsIgnoreCase("MongoDBReader")){
			chain.addProcess(new MongoDBReader());
		} else if(rtype.equalsIgnoreCase("RedisReader")){
			chain.addProcess(new RedisReader());
		}

		if (wtype.equalsIgnoreCase("MysqlWriter") || wtype.equalsIgnoreCase("OracleWriter")){
			chain.addProcess(new DBWriter());
		} else if(wtype.equalsIgnoreCase("HBaseWriter")){
			chain.addProcess(new HBaseWriter());
		} else if(wtype.equalsIgnoreCase("HdfsWriter")){
			chain.addProcess(new HdfsWriter());
		} else if(wtype.equalsIgnoreCase("HiveWriter")){
			chain.addProcess(new HiveWriter());
		} else if(wtype.equalsIgnoreCase("MongoDBWriter")){
			chain.addProcess(new MongoDBWriter());
		} else if(wtype.equalsIgnoreCase("RedisWriter")){
			chain.addProcess(new RedisWriter());
		}

		Constants.chain_map.put(pair.getId(), chain);
	}

}
