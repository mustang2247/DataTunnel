package com.bytegriffin.datatunnel.core;

public enum DataType {

    mysql("mysql"), mongodb("mongodb"), hbase("habse"), lucene("lucene"), clazz("class"), kafka("kafka"), redis("redis");

    private String value;

    DataType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
