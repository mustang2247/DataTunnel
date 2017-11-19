package com.bytegriffin.datatunnel.read;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.sql.Field;
import com.bytegriffin.datatunnel.sql.SelectObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class LuceneReader implements Readable {

	private static final Logger logger = LogManager.getLogger(LuceneReader.class);
	private static int search_count = 10;//默认查询出来的数量

	@Override
	public void channelRead(HandlerContext ctx, Param msg) {
		FSDirectory fsdir = Globals.getLuceneDir(this.hashCode());
		OperatorDefine opt = Globals.operators.get(this.hashCode());
		String newsql = SqlParser.getReadSql(opt.getValue());
        List<Record> results = select(fsdir, newsql);
        msg.setRecords(results);
        ctx.write(msg);
        logger.info("线程[{}]调用LuceneReader执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
	}

	/**
	 * 查询需要设置where条件，不支持select *，需要写具体的column name
	 * where条件中的and和or暂时不能同时使用
	 * @param dir lucene存储的绝对路径，最后一个子目录名称就是表名
	 * @param sql
	 * @return
	 */
	private List<Record> select(FSDirectory fsdir, String sql) {
        if (fsdir == null) {
            return null;
        }
        SelectObject select = SqlMapper.select(sql);
        List<Record> records = Lists.newArrayList();
        // 不支持select *，要写具体的column name
        List<String> columns = select.getColumn();
        if (columns.size() == 1 && columns.get(0).contains("*")) {
            return records;
        }
        try {
        	IndexReader indexReader = DirectoryReader.open(fsdir);
        	IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        	List<Field> where = Lists.newArrayList();
        	if (!Strings.isNullOrEmpty(select.getWhere())) {
                List<String> ands = select.getAndCondition(select.getWhere());
                List<String> ors = select.getOrCondition(select.getWhere());
                Field field = select.getLikeCondition(select.getWhere());
                if (ands != null) {//用and连接的查询条件
                	ands.forEach(con -> setQueryFilter(con, where));
                	BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    where.forEach(con -> {
                    	TermQuery query = new TermQuery(new Term(con.getFieldName().toString(), con.getFieldValue().toString()));
                        builder.add(query, BooleanClause.Occur.MUST);
                    });
                    TopDocs topDocs = indexSearcher.search(builder.build(), search_count);
                    buildRecords(topDocs, indexSearcher, columns, records);
                } else if (ors != null) {//用or连接的查询条件
                    ors.forEach(con -> setQueryFilter(con, where));
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    where.forEach(con -> {
                    	TermQuery query = new TermQuery(new Term(con.getFieldName().toString(), con.getFieldValue().toString()));
                        builder.add(query, BooleanClause.Occur.SHOULD);
                    });
                    TopDocs topDocs = indexSearcher.search(builder.build(), search_count);
                    buildRecords(topDocs, indexSearcher, columns, records);
                } else if(field != null){//用like查询，相当于like xxx%
                	setQueryFilter(select.getWhere(), where);
                	Query query=new PrefixQuery(new Term(where.get(0).getFieldName().toString(), where.get(0).getFieldValue().toString()));
                    TopDocs topDocs=indexSearcher.search(query, search_count);
                    buildRecords(topDocs, indexSearcher, columns, records);
                } else {//只存在一个查询条件
                    setQueryFilter(select.getWhere(), where);
                    Query query = new TermQuery(new Term(where.get(0).getFieldName().toString(), where.get(0).getFieldValue().toString()));
                    TopDocs topDocs = indexSearcher.search(query, search_count);
                    buildRecords(topDocs, indexSearcher, columns, records);
                }
            }

        } catch (Exception e) {
            logger.error("LuceneReader查询数据时出错: {}", sql, e);
        }
        return records;
    }

	/**
	 * 设置where条件
	 * @param condition
	 * @param where
	 */
	private void setQueryFilter(String condition, List<Field> where) {
		//获取类似 name = zhangsan 单个where条件 ，暂时不支持like/in查询
        List<String> conlist = Splitter.on("=").trimResults().omitEmptyStrings().splitToList(condition);
        String left = conlist.get(0).trim();
        String right = SqlParser.removeSqlQuotes(conlist.get(1).trim());
        where.add(new Field(left, right));
	}

	/**
	 * 构建查询记录
	 * @param topDocs
	 * @param indexSearcher
	 * @param columns
	 * @param records
	 * @throws IOException
	 */
	private void buildRecords(TopDocs topDocs,IndexSearcher indexSearcher, List<String> columns, List<Record> records) throws IOException {
		for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document document = indexSearcher.doc(scoreDoc.doc);
            List<Field> flist = Lists.newArrayList();
            columns.forEach(column -> {
            	flist.add(new Field(column, document.get(column)));
            });
            records.add(new Record(flist));
	    }
	}

}
