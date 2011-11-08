package com.taobao.common.tfs.performance.base;

@SuppressWarnings("rawtypes")
public class Utility {

	public static Class getClass(String className) {
		Class clazz = null;

		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("class not found: " + className);
		}
		return clazz;
	}

	public static Object newInstance(String className) {
		Class clazz = getClass(className);
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("new instance failed: " + className);
		}
	}

	public static boolean isEmpty(String str) {
		return str == null || str.length() == 0 || str.trim().length() == 0;
	}

	public static boolean isNotEmpty(String str) {
		return !isEmpty(str);
	}
}
