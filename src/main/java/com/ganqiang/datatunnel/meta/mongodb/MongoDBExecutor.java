package com.ganqiang.datatunnel.meta.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.ganqiang.datatunnel.core.Constants;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class MongoDBExecutor {

	private static final Logger logger = Logger
			.getLogger(MongoDBExecutor.class);

	private String poolId;

	public MongoDBExecutor(String poolId) {
		this.poolId = poolId;
	}

	public DBObject find(String collection, String _id) {
		DB db = Constants.mongodb_map.get(poolId);
		DBObject obj = new BasicDBObject();
		obj.put("_id", ObjectId.get());
		return db.getCollection(collection).findOne(obj);
	}

	public List<DBObject> find(String collection) {
		DB db = Constants.mongodb_map.get(poolId);
		return db.getCollection(collection).find().toArray();
	}

	public DBObject findOne(String collection, Map<String, Object> map) {
		DB db = Constants.mongodb_map.get(poolId);
		DBCollection coll = db.getCollection(collection);
		return coll.findOne(map2Obj(map));
	}

	public List<DBObject> find(String collection, Map<String, Object> map)
			throws Exception {
		DB db = Constants.mongodb_map.get(poolId);
		DBCollection coll = db.getCollection(collection);
		DBCursor c = coll.find(map2Obj(map));
		if (c != null)
			return c.toArray();
		else
			return null;
	}

	public BasicDBObject map2Obj(Map<String, Object> map) {
		BasicDBObject dbObject = null;
		if (!map.isEmpty()) {
			dbObject = new BasicDBObject();
		}
		for (String key : map.keySet()) {
			dbObject.put(key, map.get(key));
		}
		return dbObject;
	}

	public void insert(String collection, List<Map<String, Object>> list) {
		try {
			DB db = Constants.mongodb_map.get(poolId);
			List<DBObject> listDB = new ArrayList<DBObject>();
			for (int i = 0; i < list.size(); i++) {
				DBObject dbObject = map2Obj(list.get(i));
				listDB.add(dbObject);
			}
			db.getCollection(collection).insert(listDB);
		} catch (MongoException e) {
			logger.error("MongoDB have a fault, ", e);
		}
	}

	public void insert(String collection, Map<String, Object> map) {
		try {
			DB db = Constants.mongodb_map.get(poolId);
			DBObject dbObject = map2Obj(map);
			db.getCollection(collection).insert(dbObject);
		} catch (MongoException e) {
			logger.error("MongoDB have a fault, ", e);
		}
	}

	public void delete(String collection, List<Map<String, Object>> list) {
		if (list == null || list.isEmpty()) {
			return;
		}
		DB db = Constants.mongodb_map.get(poolId);
		for (int i = 0; i < list.size(); i++) {
			db.getCollection(collection).remove(map2Obj(list.get(i)));
		}
	}

	public void update(String collection, Map<String, Object> setFields,
			Map<String, Object> whereFields) {
		DBObject obj1 = map2Obj(setFields);
		DBObject obj2 = map2Obj(whereFields);
		DB db = Constants.mongodb_map.get(poolId);
		db.getCollection(collection).updateMulti(obj1, obj2);
	}

	public static void main(String... args) throws Exception {
		String[] str = "localhost:27017/test".split("/");
		String url = str[0];
		String database = str[1];
		System.out.println("" + url + "   " + database);
	}

}
