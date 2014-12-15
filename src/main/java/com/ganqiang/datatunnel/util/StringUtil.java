package com.ganqiang.datatunnel.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringUtil {

	public static boolean isNullOrBlank(String str) {
		if (null == str || "".equals(str.trim())) {
			return true;
		}
		return false;
	}

	public static String replaceBlank(String str) {
		String dest = "";
		if (str != null) {
			Pattern p = Pattern.compile("\\s*|\t|\r|\n");
			Matcher m = p.matcher(str);
			dest = m.replaceAll("");
		}
		return dest;
	}

}
