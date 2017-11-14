package com.bytegriffin.datatunnel.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Date;

public final class DateUtil {

    private static final Logger logger = LogManager.getLogger(DateUtil.class);
    private static final String yyyyMM = "yyyy-MM";
    private static final String yyyyMMddHH = "yyyy-MM-dd HH";
    private static final String yyyyMMddHHmmss = "yyyy-MM-dd HH:mm:ss";

    public static Date strToDate(String str) {
        DateTimeFormatter format = DateTimeFormatter.ofPattern(yyyyMMddHHmmss);
        try {
            LocalDateTime ldt = LocalDateTime.parse(str, format);
            Instant instant = ldt.atZone(ZoneId.systemDefault()).toInstant();
            return Date.from(instant);
        } catch (DateTimeParseException e) {
            logger.error("时间格式出错，正确格式为[yyyy-MM-dd HH:mm:ss]：", e);
        }
        return null;
    }

    public static String getYesterday() {
        return LocalDate.now().minusDays(1).toString();
    }

    public static String getToday() {
        return LocalDate.now().toString();
    }

    public static String getLasthour() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(yyyyMMddHH);
        return LocalDateTime.now().minusHours(1).format(formatter);
    }

    public static String getHour() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(yyyyMMddHH);
        return LocalDateTime.now().format(formatter);
    }

    public static String getLastWeekDay() {
        return LocalDate.now().minusWeeks(1).toString();
    }

    public static String getLastMonth() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(yyyyMM);
        return LocalDate.now().minusMonths(1).format(formatter);
    }

    public static String getMonth() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(yyyyMM);
        return LocalDate.now().format(formatter);
    }

    public static String getLastYear() {
        return String.valueOf(LocalDate.now().minusYears(1).getYear());
    }

    public static String getYear() {
        return String.valueOf(LocalDate.now().getYear());
    }

    public static String getLastMonthOfYear() {
        return String.valueOf(LocalDate.now().minusMonths(1).getMonthValue());
    }

    public static String getMonthOfYear() {
        return String.valueOf(LocalDate.now().getMonthValue());
    }

    public static int getWeekOfYear() {
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

    public static int getWeekOfMonth() {
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

    public static int getLastWeekOfYear() {
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
        // cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天
        // cal.set(Calendar.YEAR, yearNum);
        // cal.set(Calendar.DAY_OF_WEEK, 2);

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        // 分别取得当前日期的年、月、日
        return df.format(cal.getTime());
    }

    public static String getLastWeekEndDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DAY_OF_WEEK, 1);
        // cal.setFirstDayOfWeek(Calendar.MONDAY); // 设置每周的第一天为星期一
        // cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);// 每周从周一开始
        // 上面两句代码配合，才能实现，每年度的第一个周，是包含第一个星期一的那个周。
        // cal.setMinimalDaysInFirstWeek(7); // 设置每周最少为7天

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime());
    }

    public static String getTomorrow() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(yyyyMMddHHmmss);
        return LocalDateTime.now().plusDays(1).format(formatter).toString();
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

}
