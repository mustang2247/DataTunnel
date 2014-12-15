package com.ganqiang.datatunnel.meta.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.util.DateUtil;
import com.ganqiang.datatunnel.util.StringUtil;

public final class SqlFilter {

	private static final Logger logger = Logger.getLogger(SqlFilter.class);

	private static List<String> builtinkeys;

	static {
		builtinkeys = new ArrayList<String>();
		builtinkeys.add("yesterday");
		builtinkeys.add("today");
		builtinkeys.add("lasthour");
		builtinkeys.add("hour");
		builtinkeys.add("weekofyear");
		builtinkeys.add("weekofmonth");
		builtinkeys.add("lastweek");
		builtinkeys.add("lastweekofyear");
		builtinkeys.add("lastweekfirstday");
		builtinkeys.add("lastweekendday");
		builtinkeys.add("lastmonth");
		builtinkeys.add("month");
		builtinkeys.add("monthofyear");
		builtinkeys.add("lastmonthofyear");
		builtinkeys.add("lastyear");
		builtinkeys.add("year");
		builtinkeys.add("uuid");
	}

	/**
	 *   提取sql中的关键字
	 * 
	 * @param sql
	 * @return
	 */
	private static List<String> getKeysFromSql(String sql) {
		if (StringUtil.isNullOrBlank(sql)) {
			throw new RuntimeException("sql is null");
		}
		List<String> strlist = new ArrayList<String>();
		Pattern pattern2 = Pattern.compile("\\{[^}]+\\}");
		Matcher m2 = pattern2.matcher(sql);
		while (m2.find()) {
			String tmp = m2.group().replaceAll("[\\{\\}]", "");
			strlist.add(tmp);
		}
		return strlist;
	}

	private static String getVariableValue(String str) {
		if (StringUtil.isNullOrBlank(str)) {
			throw new RuntimeException("sql is null");
		}
		String newStr = str.trim();
		if ("yesterday".equalsIgnoreCase(newStr)) {
			return DateUtil.getYesterday();
		} else if ("today".equalsIgnoreCase(newStr)) {
			return DateUtil.getToday();
		} else if ("lasthour".equalsIgnoreCase(newStr)) {
			return DateUtil.getLasthour();
		} else if ("hour".equalsIgnoreCase(newStr)) {
			return DateUtil.getHour();
		} else if ("lastweek".equalsIgnoreCase(newStr)) {
			return DateUtil.getLastWeek();
		} else if ("lastweekday".equalsIgnoreCase(newStr)) {
			return DateUtil.getLastWeekDay();
		} else if ("lastmonth".equalsIgnoreCase(newStr)) {
			return DateUtil.getLastMonth();
		} else if ("month".equalsIgnoreCase(newStr)) {
			return DateUtil.getMonth();
		} else if ("lastyear".equalsIgnoreCase(newStr)) {
			return DateUtil.getLastYear();
		} else if ("year".equalsIgnoreCase(newStr)) {
			return DateUtil.getYear();
		} else if ("uuid".equalsIgnoreCase(newStr)) {
			return UUID.randomUUID().toString();
		} else if ("lastweekofyear".equalsIgnoreCase(newStr)) {
			return String.valueOf(DateUtil.getLastWeekOfYear());
		} else if ("lastweekfirstday".equalsIgnoreCase(newStr)) {
			return DateUtil.getLastWeekFirstDay();
		} else if ("lastweekendday".equalsIgnoreCase(newStr)) {
			return DateUtil.getLastWeekEndDay();
		} else if ("lastmonthofyear".equalsIgnoreCase(newStr)) {
			return String.valueOf(DateUtil.getLastMonthOfYear());
		} else if ("weekofyear".equalsIgnoreCase(newStr)) {
			return String.valueOf(DateUtil.getWeekOfYear());
		} else if ("weekofmonth".equalsIgnoreCase(newStr)) {
			return String.valueOf(DateUtil.getWeekOfMonth());
		} else if ("monthofyear".equalsIgnoreCase(newStr)) {
			return String.valueOf(DateUtil.getMonthOfYear());
		}
		return null;
	}

	public static String replaceSelectSql(String oldsql) {
		String newsql = oldsql;
		logger.info("Task "+Thread.currentThread().getName()	+ " before execute select filter, sql value : " + oldsql);
		List<String> sqlkeys = getKeysFromSql(oldsql);
		for (String sqlkey : sqlkeys) {
			for (String systemkey : builtinkeys) {
				if (systemkey.equalsIgnoreCase(sqlkey)) {
					String value = getVariableValue(sqlkey);
					newsql = newsql.replace("#{" + sqlkey + "}", value);
				}
			}
		}
		logger.info("Task "+Thread.currentThread().getName()	+ " after execute select filter, sql value : " + oldsql);
		return newsql;
	}

	public static List<String> replaceUpdateSql(String writerSql,	List<Map<String, Object>> selectResult) {
		logger.info("Task "+Thread.currentThread().getName()	+ " before execute update filter, sql value : " + writerSql);
		List<String> keys = getKeysFromSql(writerSql);
		List<String> sqls = new ArrayList<String>();
		for (Map<String, Object> map : selectResult) {
			String sql = writerSql;
			for (String sqlkey : keys) {
				for (String key : map.keySet()) {
					if (sqlkey.equalsIgnoreCase(key)) {
						Object columnvalue = map.get(key);
						if (columnvalue == null) {
							sql = sql.replace("${" + sqlkey + "}", "");
						} else {
							sql = sql.replace("${" + sqlkey + "}",
									columnvalue.toString());
						}
					}
				}
				for(String bkey : builtinkeys){
					if (bkey.equalsIgnoreCase(sqlkey)) {
						String newvalue = getVariableValue(sqlkey);
						sql = sql.replace("#{" + bkey + "}", newvalue);
					}
				}
			}
			sqls.add(sql);
			logger.info("Task "+Thread.currentThread().getName()	+ " after execute update filter, sql value : " + sql);
			
		}
		return sqls;
	}

	public static void main(String... args) {
		System.out.println("====" + getKeysFromSql("select #{uuid},${abc},abc from project"));

	}

}
