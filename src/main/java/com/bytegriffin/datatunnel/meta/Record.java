package com.bytegriffin.datatunnel.meta;

import com.bytegriffin.datatunnel.sql.Field;

import java.util.List;

/**
 * 数据记录
 */
public class Record {

    public Record() {

    }

    public Record(List<Field> fields) {
        this.fields = fields;
    }

    private List<Field> fields;

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}
