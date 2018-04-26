package com.bytegriffin.datatunnel.sql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeleteObject extends SqlMapper {

    private static final Logger logger = LogManager.getLogger(DeleteObject.class);
    private String tableName;
    private String where;

    public static DeleteObject create() {
        return new DeleteObject();
    }

    @Override
    public DeleteObject build(String sql) {
        Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(sql);
            Delete delete = (Delete) stmt;
            if (delete.getWhere() != null) {
                this.setWhere(delete.getWhere().toString());
            }
            this.setTableName(getTableName(stmt));
        } catch (JSQLParserException e) {
            logger.error("delete sql:[{}]解析有错误。", sql, e);
        }
        return this;
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
