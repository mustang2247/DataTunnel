package com.ganqiang.datatunnel.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;


public final class DateUtil
{
  private static final Logger logger = Logger.getLogger(DateUtil.class);
  
  public static boolean checkDate(String date)
  {
    String regex = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?" +
    		"((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))" +
    		"|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))" +
    		"|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])" +
    		"|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]" +
    		"?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]" +
    		"?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]" +
    		"?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s(((0?[0-9])" +
    		"|([1][0-9])|([2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$";
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(date);
    boolean b = m.matches();
    return b;
  }
  
  public static String dateToStr(Date date){
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return sdf.format(date);
  }
  
  public static Date strToDate(String time){
    Date date = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      date = sdf.parse(time);
    } catch (ParseException e) {
      logger.error("configuration time occur error ",e);
    }
    return date;
  }
  
  @SuppressWarnings("deprecation")
  public static Date parseStartTime(String time){
    Date date = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      date = sdf.parse(time);
    } catch (ParseException e) {
      logger.error("configuration time occur error ",e);
    }
    Calendar calendar = Calendar.getInstance();
    int year = calendar.get(date.getYear());
    int month = calendar.get(Calendar.MONTH);
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    calendar.set(year, month, day, date.getHours(), date.getMinutes(), date.getSeconds());
    return calendar.getTime();
  }
  
  public static String getYesterday(){
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE,   -1);
    String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
    return yesterday;
  }
  
  public static String getToday(){
    Calendar cal = Calendar.getInstance();
    String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
    return yesterday;
  }
  
  public static String getLasthour(){
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR_OF_DAY,   -1);
    String yesterday = new SimpleDateFormat("yyyy-MM-dd HH").format(cal.getTime());
    return yesterday;
  }
  
  public static String getHour(){
    Calendar cal = Calendar.getInstance();
    String hour = new SimpleDateFormat("yyyy-MM-dd HH").format(cal.getTime());
    return hour;
  }
  
  public static String getLastWeekDay(){
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.WEEK_OF_YEAR,   -1);
    String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
    return yesterday;
  }
  
  public static String getWeek(){
    Calendar cal = Calendar.getInstance();
    String week = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
    return week;
  }
  
  public static String getLastMonth(){
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MONTH, -1);
    String lastmonth = new SimpleDateFormat("yyyy-MM").format(cal.getTime());
    return lastmonth;
  }
  
  public static String getMonth(){
    Calendar cal = Calendar.getInstance();
    String month = new SimpleDateFormat("yyyy-MM").format(cal.getTime());
    return month;
  }
  
  public static String getLastYear(){
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.YEAR, -1);
    String lastyear = new SimpleDateFormat("yyyy").format(cal.getTime());
    return lastyear;
  }
  
  public static String getYear(){
    Calendar cal = Calendar.getInstance();
    String year = new SimpleDateFormat("yyyy").format(cal.getTime());
    return year;
  }
  
  public static String getCurrentTimeStr() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String date = sdf.format(new Date());
    return date;
  }

//  public static String strToDate(String date)
//  {
//    if (StringUtil.isNullOrBlank(date)) {
//      return getCurrentTimeStr();
//    }
//    SimpleDateFormat simpledateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//    int index = date.indexOf(".");
//    if (index != -1) {
//      date = date.substring(0, index);
//    }
//    if (date.length() < 13){ 
//      int size = 13 - date.length();
//      for (int i=0;i<size;i++) {
//        date += "0";
//      }
//    }
//    String temp = simpledateformat.format(Long.valueOf(date));
//    return temp;
//  }
  
  public static Date addDay(Date date, int num){
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.DAY_OF_MONTH, num);
    return cal.getTime();
  }
  
  public static Date addMillSecond(Date date, int num){
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MILLISECOND, num);
    return cal.getTime();
  }
  
  public static String getLastWeek(){
    try {
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.WEEK_OF_YEAR, -1);
      Date date = cal.getTime();
      cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
      cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
      cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天
      cal.setTime(date);
      int year = cal.get(Calendar.YEAR);
      int weeknum = cal.get(Calendar.WEEK_OF_YEAR);
      return year+"-"+weeknum;
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    return "";
  }
  
  public static int getLastMonthOfYear(){
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MONTH, 0);
    int lastmonth = cal.get(Calendar.MONTH);
    return lastmonth;
  }
  
  public static int getMonthOfYear(){
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MONTH, 1);
    int lastmonth = cal.get(Calendar.MONTH);
    return lastmonth;
  }
  
  public static int getWeekOfYear(){
    try {
      Calendar cal = Calendar.getInstance();
      Date date = cal.getTime();
      cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
      cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
      cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天
      cal.setTime(date);
      return cal.get(Calendar.WEEK_OF_YEAR);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    return 0;
  }
  
  public static int getWeekOfMonth(){
    try {
      Calendar cal = Calendar.getInstance();
      Date date = cal.getTime();
      cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
      cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
      cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天
      cal.setTime(date);
      return cal.get(Calendar.WEEK_OF_MONTH);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    return 0;
  }
  
  public static int getLastWeekOfYear(){
    try {
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.WEEK_OF_YEAR, -1);
      Date date = cal.getTime();
      cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
      cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
      cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天
      cal.setTime(date);
      return cal.get(Calendar.WEEK_OF_YEAR);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    return 0;
  }
  
  public static String getLastWeekFirstDay() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.WEEK_OF_YEAR, -1);
    cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
    // 上面两句代码配合，才能实现，每年度的第一个周，是包含第一个星期一的那个周。
//    cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天
//    cal.set(Calendar.YEAR, yearNum);
//    cal.set(Calendar.DAY_OF_WEEK, 2);
    
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    // 分别取得当前日期的年、月、日
    return df.format(cal.getTime());
  }
  
  public static String getLastWeekEndDay() {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_WEEK, 1);
    //cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
   // cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
    // 上面两句代码配合，才能实现，每年度的第一个周，是包含第一个星期一的那个周。
   // cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天

    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    return df.format(cal.getTime());
  }
  
  public static String getWeekNum(String str){
    try {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      Calendar cal = Calendar.getInstance();
      cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
      cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
      cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天
      cal.setTime(df.parse(str));
      int year = cal.get(Calendar.YEAR);
      int weeknum = cal.get(Calendar.WEEK_OF_YEAR);
      return year+"-"+weeknum;
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    return "";
  }
  
  
  public static String getTomorrow() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, 1);    
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 分别取得当前日期的年、月、日
    return df.format(cal.getTime());
  }
  
  public static String getNextWeekFirstDay() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.WEEK_OF_YEAR, 1);
    cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始    
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 分别取得当前日期的年、月、日
    return df.format(cal.getTime());
  }
  
  public static String getNextMonthFirstDay() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MONTH, 1);
    cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
    cal.set(Calendar.DAY_OF_MONTH, 1);// 每周从周一开始    
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 分别取得当前日期的年、月、日
    return df.format(cal.getTime());
  }

  public static void main(String... args){
    int sss = getMonthOfYear();
    System.out.println(sss);
    System.out.println(getYear());
    System.out.println(getWeekOfYear());
  }
}
