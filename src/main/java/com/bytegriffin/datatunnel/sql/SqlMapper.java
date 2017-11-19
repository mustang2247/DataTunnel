package com.bytegriffin.datatunnel.sql;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.List;
import java.util.Map;

/**
 * sql与对象的映射类
 */
public abstract class SqlMapper {

    public abstract SqlMapper build(String sql);

    public static SelectObject select(String sql) {
        return SelectObject.create().build(sql.trim());
    }

    public static InsertObject insert(String sql) {
        return InsertObject.create().build(sql.trim());
    }

    public static UpdateObject update(String sql) {
        return UpdateObject.create().build(sql.trim());
    }

    public static DeleteObject delete(String sql) {
        return DeleteObject.create().build(sql.trim());
    }

    /**
     * 返回where条件
     *
     * @return
     */
    public abstract String getWhere();

    /**
     * 返回表名，目前只支持单表
     *
     * @param stat
     * @return
     */
    String getTableName(Statement stat) {
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(stat);
        return tableList.get(0).trim();
    }
    
    /**
     * 获取某SQL中的Table　Name
     * @param sql
     * @return
     */
    public static String getTableName(String sql) {
    	if(Strings.isNullOrEmpty(sql)) {
    		return null;
    	}
    	String lowersql = sql.toLowerCase();
    	if(lowersql.contains("select")) {
    		return select(sql).getTableName();
    	} else if(lowersql.contains("insert")) {
    		return insert(sql).getTableName();
    	} else if(lowersql.contains("update")) {
    		return update(sql).getTableName();
    	} else if(lowersql.contains("delete")) {
    		return delete(sql).getTableName();
    	}
    	return null;
    }

    /**
     * 获取where条件中的字段名与字段值
     *
     * @param condition
     * @return
     */
    public static List<Field> getWhereFields(String condition) {
        List<String> conlist = Splitter.on("=").trimResults().omitEmptyStrings().splitToList(condition);
        String left = conlist.get(0).toLowerCase().trim();
        String right = SqlParser.removeSqlQuotes(conlist.get(1).trim());
        List<Field> list = Lists.newArrayList();
        list.add(new Field(left, right));
        return list;
    }

    /**
     * 获取and连接的where条件，例如：name=abc / age=13
     *
     * @param where
     * @return
     */
    public List<String> getAndCondition(String where) {
        if (where.toLowerCase().contains("and")) {
            return Splitter.on("and").trimResults().omitEmptyStrings().splitToList(where.toLowerCase());
        }
        return null;
    }

    /**
     * 获取or连接的where条件
     *
     * @param where
     * @return
     */
    public List<String> getOrCondition(String where) {
        if (where.toLowerCase().contains("or")) {
            return Splitter.on("or").trimResults().omitEmptyStrings().splitToList(where.toLowerCase());
        }
        return null;
    }

    /**
     * 获取包含like的where条件
     * 注意：暂时只支持一个like条件
     * @param where
     * @return
     */
    public Field getLikeCondition(String where) {
        if (where.toLowerCase().contains("like")) {
        	List<String> list = Splitter.on("like").trimResults().omitEmptyStrings()
        			.splitToList(where.toLowerCase());
        	return new Field(list.get(0), list.get(1));
        }
        return null;
    }

    /**
     * 返回where条件
     *
     * @param where
     * @return
     */
    @Deprecated
    public List<Field> getWhere(String where) {
        if (Strings.isNullOrEmpty(where)) {
            return null;
        }
        List<Field> fields = Lists.newArrayList();
        if (where.contains("(")) {
            String subwhere = where.substring(where.indexOf("(") + 1, where.lastIndexOf(")"));
            fields.addAll(buildFields(subwhere));
            where = where.replace("(" + subwhere + ")", "");
        }
        fields.addAll(buildFields(where));
        return fields;
    }

    /**
     * 构建字段
     *
     * @param where
     * @return
     */
    @Deprecated
    private List<Field> buildFields(String where) {
        String subwhere = where.contains("(") ? where.substring(where.indexOf("(") + 1, where.lastIndexOf(")")) : where;
        List<Field> fields = Lists.newArrayList();
        if (subwhere.toLowerCase().contains("and")) {
            Map<String, String> map = Splitter.on("and").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(subwhere.toLowerCase());
            map.forEach((k, v) -> fields.add(new Field(k, v)));
        }
        if (subwhere.toLowerCase().contains("or")) {
            Map<String, String> map = Splitter.on("or").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(subwhere.toLowerCase());
            map.forEach((k, v) -> fields.add(new Field(k, v)));
        }
        return fields;
    }

}
