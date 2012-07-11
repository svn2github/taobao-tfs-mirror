package com.taobao.common.tfs.utility;

public class TimeUtility {
	
	public static void sleep(int time) 
	{
		try 
		{
			Thread.sleep(time * 1000L);
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
