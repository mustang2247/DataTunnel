package com.bytegriffin.datatunnel.read;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.MongoDBContext;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.SelectObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.List;

public class MongoDBReader implements Readable {

    private static final Logger logger = LogManager.getLogger(MongoDBReader.class);

    @Override
    public void channelRead(HandlerContext ctx, Param msg) {
        MongoDatabase database = Globals.getMongoDatabase(this.hashCode());
        OperatorDefine opt = Globals.operators.get(this.hashCode());
        String newsql = SqlParser.getReadSql(opt.getValue());
        List<Record> results = select(database, newsql);
        msg.setRecords(results);
        ctx.write(msg);
        logger.info("线程[{}]调用MongoDBReader执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
    }

    /**
     * sql 查询引擎，类似：select name, age from student where b =3  <br>
     * 目前版本查询支持简单的单表查询，且and和or不能同时出现  <br>
     *
     * @param connection
     * @param sql
     * @return
     */
    private List<Record> select(MongoDatabase database, String sql) {
        if (Strings.isNullOrEmpty(sql)) {
            return null;
        }
        SelectObject select = SqlMapper.select(sql);
        List<Record> list = Lists.newArrayList();
        try {
            MongoCollection<Document> collection = database.getCollection(SqlMapper.select(sql).getTableName());
            //1.设置要查询的where条件
            Document searchQuery = MongoDBContext.setQueryFilters(select);

            List<String> columns = select.getColumn();
            // select * 不支持，要写具体的column name
            if (columns.size() == 1 && columns.get(0).contains("*")) {
                columns.clear();
            }
            Document projection = new Document();
            //_id字段默认会显示
            if (columns.contains("_id")) {
                projection.append("_id", 0);
            }
            columns.forEach(column -> {
                //0表示此列不显示，除此列之外全显示；1表示此列显示，除此列之外全不显示
                projection.append(column, 1);
            });
            //2.设置要查询的字段，类似 select name from table 中的name
            collection.find(searchQuery).projection(projection).iterator().forEachRemaining(result -> {
                Record record = new Record();
                List<Field> fields = Lists.newArrayList();
                columns.forEach(column -> {
                    fields.add(new Field(column, result.get(column)));
                });
                record.setFields(fields);
                list.add(record);
            });

        } catch (Exception e) {
            logger.error("HBaseReader查询数据时出错: {}", sql, e);
        }
        return list;
    }

}
