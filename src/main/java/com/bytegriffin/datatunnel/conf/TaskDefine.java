package com.bytegriffin.datatunnel.conf;

import java.util.List;

public class TaskDefine {

	private String name;
	private String operation;
	private String interval;
	private String startTime;
	private List<OperatorDefine> readerDefines;
	private List<OperatorDefine> writerDefines;

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<OperatorDefine> getReaderDefines() {
		return readerDefines;
	}
	public void setReaderDefines(List<OperatorDefine> readerDefines) {
		this.readerDefines = readerDefines;
	}
	public List<OperatorDefine> getWriterDefines() {
		return writerDefines;
	}
	public void setWriterDefines(List<OperatorDefine> writerDefines) {
		this.writerDefines = writerDefines;
	}
	public String getInterval() {
		return interval;
	}
	public void setInterval(String interval) {
		this.interval = interval;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
}
