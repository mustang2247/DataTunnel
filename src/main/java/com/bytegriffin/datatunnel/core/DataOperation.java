package com.bytegriffin.datatunnel.core;

public enum DataOperation {

	stat("stat"), sync("sync"),clean("clean"),forward("forward");

	private String value;

	DataOperation(String value){
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
