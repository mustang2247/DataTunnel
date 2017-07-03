package com.bytegriffin.datatunnel.core;

import java.util.List;

import com.bytegriffin.datatunnel.conf.TaskDefine;
import com.bytegriffin.datatunnel.meta.Record;

public class Param {

	private TaskDefine taskdefine;
	private List<Record> results;

	public TaskDefine getTaskdefine() {
		return taskdefine;
	}

	public void setTaskdefine(TaskDefine taskdefine) {
		this.taskdefine = taskdefine;
	}

	public List<Record> getResults() {
		return results;
	}

	public void setResults(List<Record> results) {
		this.results = results;
	}

}
