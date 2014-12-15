package com.ganqiang.datatunnel.core;

public enum DataPoolType {

	MysqlReader("MysqlReader"), MysqlWriter("MysqlWriter"), OracleReader("OracleReader"), OracleWriter("OracleWriter"), 
	HBaseReader("HBaseReader"), HBaseWriter("HBaseWriter"), HiveReader("HiveReader"), HiveWriter("HiveWriter"),
	HdfsReader("HdfsReader"), HdfsWriter("HdfsWriter"), LuceneReader("LuceneReader"), LuceneWriter("LuceneWriter"),
	MongoDBReader("MongoDBReader"), MongoDBWriter("MongoDBWriter"), RedisReader("RedisReader"), RedisWriter("RedisWriter");

	private String value;

	DataPoolType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
	


}
