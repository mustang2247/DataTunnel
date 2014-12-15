package com.ganqiang.datatunnel.conf;

import java.util.Date;
import java.util.List;

public class Task {
	
	private String id;
//	private boolean isTimer;
	private Integer threadNum;
	private Long interval;
	private Date startTime;
	private List<Pair> pairs;
	
//	public boolean isTimer() {
//		return isTimer;
//	}

	public Integer getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(Integer threadNum) {
		this.threadNum = threadNum;
	}

//	public void setTimer(boolean isTimer) {
//		this.isTimer = isTimer;
//	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getInterval() {
		return interval;
	}

	public void setInterval(Long interval) {
		this.interval = interval;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public List<Pair> getPairs() {
		return pairs;
	}

	public void setPairs(List<Pair> pairs) {
		this.pairs = pairs;
	}

	public static class Pair{
		private String id;
		private String readerType;
		private String readerPoolId;
		private String readerValue;
		private String writerType;
		private String writerPoolId;
		private String writerValue;
		
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getReaderType() {
			return readerType;
		}
		public void setReaderType(String readerType) {
			this.readerType = readerType;
		}
		public String getReaderPoolId() {
			return readerPoolId;
		}
		public void setReaderPoolId(String readerPoolId) {
			this.readerPoolId = readerPoolId;
		}
		public String getReaderValue() {
			return readerValue;
		}
		public void setReaderValue(String readerValue) {
			this.readerValue = readerValue;
		}
		public String getWriterType() {
			return writerType;
		}
		public void setWriterType(String writerType) {
			this.writerType = writerType;
		}
		public String getWriterPoolId() {
			return writerPoolId;
		}
		public void setWriterPoolId(String writerPoolId) {
			this.writerPoolId = writerPoolId;
		}
		public String getWriterValue() {
			return writerValue;
		}
		public void setWriterValue(String writerValue) {
			this.writerValue = writerValue;
		}
		
	}

}
