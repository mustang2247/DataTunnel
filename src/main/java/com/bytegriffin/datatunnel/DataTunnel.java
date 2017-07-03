package com.bytegriffin.datatunnel;

import com.bytegriffin.datatunnel.core.TaskManager;

public class DataTunnel {

	 public static void main(String[] args) {
		 TaskManager.create().loads().buildSync();
	 }

}
