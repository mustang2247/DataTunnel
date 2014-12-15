package com.ganqiang.datatunnel.core;

import java.util.List;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.conf.Task;
import com.ganqiang.datatunnel.conf.Task.Pair;

public class Worker implements Runnable {

	private static final Logger log = Logger.getLogger(Worker.class);

	private Task task;

	public Worker(Task task){
		this.task = task;
	}

	@Override
	public void run() {
		List<Pair> lists = task.getPairs();
		for (Pair pair : lists) {
			Chain chain = Constants.chain_map.get(pair.getId());
			
			Param param = new Param();
			param.setPair(pair);
			chain.execute(param);
		}
		
		log.info("Worker [" + Thread.currentThread().getName() + "] execute task [" + task.getId() + "].");
	}

}