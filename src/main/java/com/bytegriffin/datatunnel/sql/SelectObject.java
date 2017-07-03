package com.bytegriffin.datatunnel.sql;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectObject extends SqlMapper{

	private static final Logger logger = LogManager.getLogger(SelectObject.class);
	private List<String> column;
	private String where;
	private String tableName;

	public static SelectObject create(){
		return new SelectObject();
	}

	public SelectObject build(String sql){
		Statement stmt;
		try {
			stmt = CCJSqlParserUtil.parse(sql);
			Select select = (Select) stmt;
			PlainSelect ps = (PlainSelect) select.getSelectBody();
			List<SelectItem> sis = ps.getSelectItems();
			this.column = Lists.newArrayList();
			sis.forEach(x -> this.column.add(x.toString()));
			if(ps.getWhere() != null){
				this.setWhere(ps.getWhere().toString());
			}
			this.setTableName(getTableName(stmt));
		} catch (JSQLParserException e) {
			logger.error("select sql:[{}]解析有错误。",sql, e);
		}
		return this;
	}

	public boolean isExistWhere(){
		return where != null && !where.isEmpty() ;
	}

	public List<String> getColumn() {
		return column;
	}
	public void setColumn(List<String> column) {
		this.column = column;
	}

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
