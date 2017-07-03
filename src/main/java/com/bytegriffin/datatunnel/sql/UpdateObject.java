package com.bytegriffin.datatunnel.sql;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;

public class UpdateObject extends SqlMapper{

	private static final Logger logger = LogManager.getLogger(UpdateObject.class);
	private List<Field> fields;
	private String tableName;
	private String where;

	public static UpdateObject create(){
		return new UpdateObject();
	}

	@Override
	public UpdateObject build(String sql) {
		Statement stmt;
		try {
			stmt = CCJSqlParserUtil.parse(sql);
			Update update = (Update) stmt;
			List<Column> columns = update.getColumns();
			List<Expression> values = update.getExpressions();
			this.fields = Lists.newArrayList();
			for (int i = 0; i < columns.size(); i++) {
				this.fields.add(new Field(columns.get(i).toString(), values.get(i).toString()));
			}
			if(update.getWhere() != null){
				this.setWhere(update.getWhere().toString());
			}
			this.setTableName(getTableName(stmt));
		} catch (JSQLParserException e) {
			logger.error("update sql:[{}]解析有错误。",sql, e);
		}
		return this;
	}

	public List<Field> getFields() {
		return fields;
	}
	public void setFields(List<Field> fields) {
		this.fields = fields;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getWhere() {
		return where;
	}
	public void setWhere(String where) {
		this.where = where;
	}
	
}
