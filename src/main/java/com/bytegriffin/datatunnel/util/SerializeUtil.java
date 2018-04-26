package com.bytegriffin.datatunnel.util;

import com.bytegriffin.datatunnel.meta.Record;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public class SerializeUtil {

	/**
	 * 序列化 Record ===> byte[]
	 * 
	 * @param record
	 * @return
	 */
	public static byte[] serialize(Record record) {
		LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		try {
			Schema<Record> schema = RuntimeSchema.getSchema(Record.class);
			return ProtostuffIOUtil.toByteArray(record, schema, buffer);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		} finally {
			buffer.clear();
		}
	}

	/**
	 * 反序列化 byte[] ===> Record
	 * 
	 * @param data
	 * @return
	 */
	public static Record deserialize(byte[] data) {
		try {
			Schema<Record> schema = RuntimeSchema.getSchema(Record.class);
			Record record = schema.newMessage();
			ProtostuffIOUtil.mergeFrom(data, record, schema);
			return record;
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	
}
