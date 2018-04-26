package com.bytegriffin.datatunnel.conf;

import com.bytegriffin.datatunnel.read.Readable;
import com.bytegriffin.datatunnel.util.MD5Util;
import com.bytegriffin.datatunnel.write.Writeable;

public class OperatorDefine {

    private String id;
    public static final String reader_opt_id = "reader";
    public static final String writer_opt_id = "writer";

    private String name;
    private String type;
    private String address;
    private String value;
    private Readable reader;
    private Writeable writer;


    public boolean isWriter() {
        return this.id.contains(writer_opt_id);
    }

    public boolean isReader() {
        return this.id.contains(reader_opt_id);
    }

    public String getKey() {
        return name + "-" + MD5Util.generateKey(address);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Readable getReader() {
        return reader;
    }

    public void setReader(Readable reader) {
        this.reader = reader;
    }

    public Writeable getWriter() {
        return writer;
    }

    public void setWriter(Writeable writer) {
        this.writer = writer;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
