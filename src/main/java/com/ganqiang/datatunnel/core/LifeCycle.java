package com.ganqiang.datatunnel.core;

import java.util.ArrayList;
import java.util.List;

public class LifeCycle implements Chain {

	List<Process> list = new ArrayList<Process>();

	@Override
	public void execute(Param param) {
		for (Process p : list) {
			p.execute(param);
		}
	}

	@Override
	public Chain addProcess(Process p) {
		list.add(p);
		return this;
	}

}
