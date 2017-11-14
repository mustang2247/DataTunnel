package com.bytegriffin.datatunnel.sql;

import com.bytegriffin.datatunnel.conf.OperatorDefine;
import com.bytegriffin.datatunnel.meta.Initializer;
import com.bytegriffin.datatunnel.meta.Record;
import com.bytegriffin.datatunnel.util.DateUtil;
import com.bytegriffin.datatunnel.util.MD5Util;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlParser implements Initializer {

    private static final Logger logger = LogManager.getLogger(SqlParser.class);

    public static SqlParser create() {
        return new SqlParser();
    }

    private static List<String> system_sql_variables;

    @Override
    public void init(OperatorDefine operator) {
        system_sql_variables = Lists.newArrayList("yesterday", "today", "lasthour", "hour", "weekofyear", "weekofmonth"
                , "lastweekofyear", "lastweekfirstday", "lastweekendday", "lastmonth", "lastmonthofyear", "lastyear",
                "month", "monthofyear", "year", "uuid");
    }

    /**
     * 内存中是否已经加载了系统级sql变量
     *
     * @return
     */
    public static boolean isLoadedSystemSqlVariables() {
        return system_sql_variables != null && !system_sql_variables.isEmpty();
    }

    /**
     * 提取sql中的变量，包括系统自定义变量和用户自定义变量 <br>
     * 其中，用户自定义变量以$开头。
     *
     * @param sql
     * @return
     */
    private static List<String> getVariableFromSql(String sql) {
        if (Strings.isNullOrEmpty(sql)) {
            return null;
        }
        List<String> strlist = Lists.newArrayList();
        Pattern pattern2 = Pattern.compile("\\{[^}]+\\}");
        Matcher m2 = pattern2.matcher(sql);
        while (m2.find()) {
            String tmp = m2.group().replaceAll("[\\{\\}]", "");
            strlist.add(tmp);
        }
        return strlist;
    }

    /**
     * 获取系统自定义sql变量，变量前缀以井号#开头
     *
     * @param str
     * @return
     */
    private static String getSystemVariable(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return null;
        }
        String newStr = str.trim();
        switch (newStr) {
            case "yesterday":
                newStr = DateUtil.getYesterday();
                break;
            case "today":
                newStr = DateUtil.getToday();
                break;
            case "lasthour":
                newStr = DateUtil.getLasthour();
                break;
            case "hour":
                newStr = DateUtil.getHour();
                break;
            case "lastweekday":
                newStr = DateUtil.getLastWeekDay();
                break;
            case "lastmonth":
                newStr = DateUtil.getLastMonth();
                break;
            case "month":
                newStr = DateUtil.getMonth();
                break;
            case "lastyear":
                newStr = DateUtil.getLastYear();
                break;
            case "year":
                newStr = DateUtil.getYear();
                break;
            case "lastweekofyear":
                newStr = String.valueOf(DateUtil.getLastWeekOfYear());
                break;
            case "lastweekfirstday":
                newStr = DateUtil.getLastWeekFirstDay();
                break;
            case "lastweekendday":
                newStr = DateUtil.getLastWeekFirstDay();
                break;
            case "lastmonthofyear":
                newStr = String.valueOf(DateUtil.getLastMonthOfYear());
                break;
            case "weekofyear":
                newStr = String.valueOf(DateUtil.getWeekOfYear());
                break;
            case "weekofmonth":
                newStr = String.valueOf(DateUtil.getWeekOfMonth());
                break;
            case "monthofyear":
                newStr = String.valueOf(DateUtil.getMonthOfYear());
                break;
            case "uuid":
                newStr = MD5Util.uuid();
                break;
            default:
                break;
        }
        return newStr;
    }

    /**
     * 过滤输入sql，一般用于select操作，将系统自定义变量替换掉
     *
     * @param readSql
     * @return
     */
    public static String getReadSql(String readSql) {
        List<String> variables = getVariableFromSql(readSql);
        if (variables == null || variables.isEmpty()) {
            return readSql;
        }
        return variables.stream()
                .filter(variable -> system_sql_variables != null)
                .filter(variable -> system_sql_variables.contains(variable.toLowerCase()))
                .map(variable -> readSql.replace("#{" + variable + "}", getSystemVariable(variable)))
                .findFirst().get();
    }

    /**
     * 过滤输出sql，一般为insert操作或update操作
     *
     * @param selectResults
     * @param writerSql
     * @return
     */
    public static List<String> getWriteSql(List<Record> selectResults, String writerSql) {
        logger.debug("线程[{}]转换前的sql值为[{}] ", Thread.currentThread().getName(), writerSql);
        List<String> variables = getVariableFromSql(writerSql);
        List<String> sqls = Lists.newArrayList();
        for (Record result : selectResults) {
            String sql = writerSql;
            for (String sqlkey : variables) {
                for (Field field : result.getFields()) {
                    if (sqlkey.equalsIgnoreCase(field.getFieldName())) {
                        Object columnvalue = field.getFieldValue();
                        if (columnvalue == null) {
                            sql = sql.replace("${" + sqlkey + "}", "");
                        } else {
                            sql = sql.replace("${" + sqlkey + "}", columnvalue.toString());
                        }
                    }
                }
                if (system_sql_variables.contains(sqlkey.toLowerCase())) {
                    sql = sql.replace("#{" + sqlkey + "}", getSystemVariable(sqlkey));
                }
            }
            sqls.add(sql);
            logger.debug("线程[{}]转换后的sql值为[{}] ", Thread.currentThread().getName(), sql);
        }
        return sqls;
    }

    /**
     * 去除sql中字段值的单引号，例如：name='zhangsan'==>name=zhangsan
     *
     * @param str
     * @return
     */
    public static String removeSqlQuotes(String str) {
        if (str.trim().startsWith("'") && str.trim().endsWith("'")) {
            str = str.substring(1, str.length());
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

}
