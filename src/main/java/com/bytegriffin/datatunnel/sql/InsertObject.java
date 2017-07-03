package com.bytegriffin.datatunnel.sql;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;

public class InsertObject  extends SqlMapper{

	private static final Logger logger = LogManager.getLogger(InsertObject.class);
	private List<Field> fields;
	private String tableName;
	
	public static InsertObject create(){
		return new InsertObject();
	}

	@Override
	public InsertObject build(String sql) {
		Statement stmt;
		try {
			stmt = CCJSqlParserUtil.parse(sql);
			Insert insert = (Insert) stmt;
			List<Column> columns = insert.getColumns();
			List<Expression> values = ((ExpressionList) insert.getItemsList()).getExpressions();
			this.fields = Lists.newArrayList();
			for (int i = 0; i < columns.size(); i++) {
				this.fields.add(new Field(columns.get(i).toString(), values.get(i).toString()));
			}
			this.setTableName(getTableName(stmt));
		} catch (JSQLParserException e) {
			logger.error("insert sql:[{}]解析有错误。",sql, e);
		}
		return this;
	}
	
	@Override
	public String getWhere() {
		return null;
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

}
