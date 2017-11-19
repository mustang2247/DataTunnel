package com.bytegriffin.datatunnel.write;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.MongoDBContext;
import com.bytegriffin.datatunnel.sql.*;
import com.google.common.collect.Lists;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.List;

public class MongoDBWriter implements Writeable {

    private static final Logger logger = LogManager.getLogger(MongoDBWriter.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        MongoDatabase database = Globals.getMongoDatabase(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        List<String> sqls = SqlParser.getWriteSql(msg.getRecords(), opt.getValue());
        write(database, sqls);
        logger.info("线程[{}]调用MongoDBWriter执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    /**
     * 向MongoDB数据库中批量写多条数据
     *
     * @param dataSource
     * @param sqls
     */
    private void write(MongoDatabase database, List<String> sqls) {
        if (database == null || sqls == null || sqls.isEmpty()) {
            return;
        }
        try {
            String firstSql = sqls.get(0).toLowerCase().trim();
            if (firstSql.contains("delete")) {//delete操作
                MongoCollection<Document> collection = database.getCollection(SqlMapper.delete(firstSql).getTableName());
                sqls.forEach(sql -> {
                    DeleteObject deleteobj = SqlMapper.delete(sql);
                    Document searchQuery = MongoDBContext.setQueryFilters(deleteobj);
                    collection.deleteMany(searchQuery);
                });
            } else if (firstSql.contains("insert")) {
                MongoCollection<Document> collection = database.getCollection(SqlMapper.insert(firstSql).getTableName());
                List<Document> inputdocs = Lists.newArrayList();
                sqls.forEach(sql -> {
                    InsertObject inputobj = SqlMapper.insert(sql);
                    Document doc = new Document();
                    inputobj.getFields().forEach(field -> {
                        doc.append(field.getFieldName(), SqlParser.removeSqlQuotes(field.getFieldValue().toString()));
                    });
                    inputdocs.add(doc);
                });
                collection.insertMany(inputdocs);
            } else if (firstSql.contains("update")) {//update操作
                MongoCollection<Document> collection = database.getCollection(SqlMapper.update(firstSql).getTableName());
                sqls.forEach(sql -> {
                    Document updatedocs = new Document();
                    UpdateObject updateobj = SqlMapper.update(sql);
                    updateobj.getFields().forEach(field -> {
                        updatedocs.append(field.getFieldName(), SqlParser.removeSqlQuotes(field.getFieldValue().toString()));
                    });
                    UpdateObject firstUpdate = SqlMapper.update(sql);
                    collection.updateMany(MongoDBContext.setQueryFilters(firstUpdate), new Document("$set", updatedocs));
                });
            }
        } catch (Exception e) {
            logger.error("不能执行更新sql: [{}]", sqls, e);
        }
    }

}
