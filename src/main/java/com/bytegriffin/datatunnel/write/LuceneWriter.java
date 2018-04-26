package com.bytegriffin.datatunnel.write;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.core.Globals;
import com.bytegriffin.datatunnel.core.HandlerContext;
import com.bytegriffin.datatunnel.core.Param;
import com.bytegriffin.datatunnel.sql.DeleteObject;
import com.bytegriffin.datatunnel.sql.InsertObject;
import com.bytegriffin.datatunnel.sql.SqlMapper;
import com.bytegriffin.datatunnel.sql.SqlParser;
import com.bytegriffin.datatunnel.sql.UpdateObject;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public class LuceneWriter implements Writeable {

	private static final Logger logger = LogManager.getLogger(LuceneWriter.class);
	private static final Analyzer analyzer = new SmartChineseAnalyzer(); // 中文分词

	@Override
	public void channelRead(HandlerContext ctx, Param msg) {
		FSDirectory fsdir = Globals.getLuceneDir(this.hashCode());
		OperatorDefine opt = Globals.operators.get(this.hashCode());
		List<String> sqls = SqlParser.getWriteSql(msg.getRecords(), opt.getValue());
		write(fsdir, sqls);
		logger.info("线程[{}]调用LuceneWriter执行任务[{}]", Thread.currentThread().getName(), opt.getKey());
	}

	/**
	 * 更新操作
	 * 注意：update操作会将原来不变的字段删除掉，因此最好全写到set后
	 * @param fsdir
	 * @param sqls
	 */
	@SuppressWarnings("resource")
	private void write(FSDirectory fsdir, List<String> sqls) {
		if (fsdir == null || sqls == null || sqls.isEmpty()) {
			return;
		}
		try {
			IndexWriter indexWriter = new IndexWriter(fsdir, buildConfig());
			String firstSql = sqls.get(0).toLowerCase().trim();
			if (firstSql.contains("delete")) {// delete操作：where条件只能设置为一个，用于设置Term参数
				for(String sql : sqls) {
					DeleteObject deleteobj = SqlMapper.delete(sql);
					if (Strings.isNullOrEmpty(deleteobj.getWhere())) {
                        logger.error("where条件中必须包含row字段，请重新设置。");
                        return;
                    }
		            indexWriter.deleteDocuments(new Term(getWhere(deleteobj.getWhere())[0], getWhere(deleteobj.getWhere())[1]));
				}
	            indexWriter.forceMergeDeletes();
	            indexWriter.commit();
			} else if (firstSql.contains("insert")) {
				for(String sql : sqls) {
					InsertObject inputobj = SqlMapper.insert(sql);
					Document doc = new Document();
					inputobj.getFields().forEach(field -> {
						doc.add(new StringField(field.getFieldName(), SqlParser.removeSqlQuotes(field.getFieldValue().toString()), Field.Store.YES));
                    });
					indexWriter.addDocument(doc);
				}
				indexWriter.forceMergeDeletes();
	            indexWriter.commit();
			} else if (firstSql.contains("update")) {// update操作：where条件只能设置为一个，用于设置Term参数
				for(String sql : sqls) {
					Document doc = new Document();
					UpdateObject updateobj = SqlMapper.update(sql);
                    updateobj.getFields().forEach(field -> {
                    	doc.add(new StringField(field.getFieldName(), SqlParser.removeSqlQuotes(field.getFieldValue().toString()), Field.Store.YES));
                    });
                    //where条件
                    doc.add(new StringField(getWhere(updateobj.getWhere())[0], getWhere(updateobj.getWhere())[1], Field.Store.YES));
		            indexWriter.updateDocument(new Term(getWhere(updateobj.getWhere())[0], getWhere(updateobj.getWhere())[1]), doc);
				}
	            indexWriter.forceMergeDeletes();
	            indexWriter.commit();
			}
		} catch (Exception e) {
			logger.error("不能执行更新sql: [{}]", sqls, e);
		}
	}

	/**
	 * 设置IndexWriter的配置参数
	 * @return
	 */
	private IndexWriterConfig buildConfig() {
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE_OR_APPEND);
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMergeFactor(20);
        config.setMergePolicy(policy);
        config.setRAMBufferSizeMB(100);
        config.setUseCompoundFile(false);
        return config;
	}

	/**
	 * 拆分where条件
	 * @param where
	 * @return
	 */
	private String[] getWhere(String where) {
		//获取类似 name = zhangsan 单个where条件 ，暂时不支持like/in查询
        List<String> conlist = Splitter.on("=").trimResults().omitEmptyStrings().splitToList(where);
        String left = conlist.get(0).trim();
        String right = SqlParser.removeSqlQuotes(conlist.get(1).trim());
        return new String[]{left, right};
	}

}
