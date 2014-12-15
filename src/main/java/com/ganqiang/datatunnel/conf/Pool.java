package com.ganqiang.datatunnel.conf;

public class Pool {

	/*** DB ***/
	private String id;//唯一ID，用于区分不同的数据源
	private String type;//数据源Reader/Writer类型
	private String url;
	private String userName;
	private String password;
	private int poolSize = 10;
	private boolean open = false;
	/*** HBase ***/
	private String quorum;
	private String clientPort;

	public String getQuorum() {
		return quorum;
	}
	public void setQuorum(String quorum) {
		this.quorum = quorum;
	}
	public String getClientPort() {
		return clientPort;
	}
	public void setClientPort(String clientPort) {
		this.clientPort = clientPort;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public int getPoolSize() {
		return poolSize;
	}
	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}
	public boolean isOpen() {
		return open;
	}
	public void setOpen(boolean open) {
		this.open = open;
	}
	
}
