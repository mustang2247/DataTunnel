package com.ganqiang.datatunnel.core;

import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ganqiang.datatunnel.conf.Task;
import com.ganqiang.datatunnel.core.Worker;

public class TaskController extends TimerTask {

	private Task task;
	
	public TaskController(Task task){
		this.task = task;
	}

	@Override
	public void run() {
		startup();
	}

	private void startup() {
		Integer thread_num = task.getThreadNum();
		if (thread_num == null){
			thread_num = Runtime.getRuntime().availableProcessors();
		}
		if (thread_num == 1) {
			Worker worker = new Worker(task);
			worker.run();
		} else {
			ExecutorService executorService = Executors.newCachedThreadPool();
			for (int i = 0; i < thread_num; i++) {
				Worker worker = new Worker(task);
				executorService.execute(worker);
			}
			// executorService.shutdown();
		}
	}

}
