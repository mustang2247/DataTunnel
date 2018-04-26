package com.bytegriffin.datatunnel.core;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.core.util.Throwables;

import java.util.List;
import java.util.Map;

/**
 * 异常捕捉
 */
public final class ExceptionCatcher {

    /**
     * 与具体任务抓取相关的异常信息
     * 当 key：task_name value：exception information
     */
    private static Map<String, List<String>> task_exception_info = Maps.newHashMap();

    /**
     * 其他类型的异常信息
     */
    private static List<String> exception_infos = Lists.newArrayList();

    public static List<String> getAllExceptions() {
        return exception_infos;
    }

    public static List<String> getExceptions(String seedName) {
        return task_exception_info.get(seedName);
    }

    /**
     * 每次抓取完成后都要清空一次异常缓存
     */
    public static void clearExceptions() {
        task_exception_info.clear();
    }

    /**
     * 获取完整的堆栈信息
     *
     * @param t
     * @return
     */
    public static String getStackTrace(Throwable t) {
        List<String> ls = Throwables.toStringList(t);
        return Joiner.on(System.lineSeparator()).join(ls);
    }

    /**
     * 增加异常信息 调用入口一
     *
     * @param seedName
     * @param t
     */
    public static void addException(String seedName, Throwable t) {
        String exception = getStackTrace(t);
        addException(seedName, exception);
    }

    /**
     * 增加异常信息 调用入口二
     *
     * @param seedName
     * @param exception
     */
    public static void addException(String seedName, String exception) {
        List<String> list = task_exception_info.get(seedName);
        if (list == null || list.size() == 0) {
            list = Lists.newArrayList();
            task_exception_info.put(seedName, list);
        }
        if (!list.contains(exception)) {
            list.add(exception);
        }
    }

    /**
     * 增加异常信息 调用入口三
     *
     * @param t
     */
    public static void addException(Throwable t) {
        String exception = getStackTrace(t);
        addException(exception);
    }

    /**
     * 增加异常信息 调用入口四
     *
     * @param exception
     */
    public static void addException(String exception) {
        if (!exception_infos.contains(exception)) {
            exception_infos.add(exception);
        }
    }
}
