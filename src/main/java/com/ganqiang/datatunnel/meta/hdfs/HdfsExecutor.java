package com.ganqiang.datatunnel.meta.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.ganqiang.datatunnel.core.Constants;

public class HdfsExecutor {

	private String poolId;

	public HdfsExecutor(String poolId){
		this.poolId = poolId;
	}

	public String read(String path) {
		FileSystem hdfs = Constants.hdfs_map.get(poolId);
		String content = null;
		Path dst = new Path(path);
		try {
			FSDataInputStream in = hdfs.open(dst);
			OutputStream os = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, os, 4096, true);
			content = os.toString();
			os.close();
			in.close();
		} catch (Exception e) {
		}
		return content;
	}
	
	public void writeLine(String path, String data) {
		FileSystem hdfs = Constants.hdfs_map.get(poolId);
		FSDataOutputStream dos = null;
		try {
			Path dst = new Path(path);
			hdfs.createNewFile(dst);
			dos = hdfs.append(new Path(path));
			dos.writeBytes(data + "\r\n");
			dos.close();
		} catch (Exception e) {
		}
	}

}
