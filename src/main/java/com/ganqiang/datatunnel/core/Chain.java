package com.ganqiang.datatunnel.core;

public interface Chain {

	Chain addProcess(Process p);
	
	void execute(Param param);
	
}
