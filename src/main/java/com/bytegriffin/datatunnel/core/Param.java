package com.bytegriffin.datatunnel.core;

import com.bytegriffin.datatunnel.conf.TaskDefine;
import com.bytegriffin.datatunnel.meta.Record;

import java.util.List;

public class Param {

    private TaskDefine taskDefine;
    private List<Record> records;

    public TaskDefine getTaskDefine() {
        return taskDefine;
    }

    public void setTaskDefine(TaskDefine taskDefine) {
        this.taskDefine = taskDefine;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

}
