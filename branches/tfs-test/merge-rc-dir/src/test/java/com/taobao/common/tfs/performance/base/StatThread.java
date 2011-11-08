package com.taobao.common.tfs.performance.base;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


public class StatThread extends Thread 
{

	private long sleepTime = 10000;
	private TestRunner runner;
	public static Logger log = Logger.getLogger("tfs_stat_thread");

	public void setSleepTime(long sleepTime) 
	{
		this.sleepTime = sleepTime;
	}

	public void setRunner(TestRunner runner) 
	{
		this.runner = runner;
	}

	@Override
	public void run() 
	{
		//log.info("线程statThread 运行中");
		Map<String, TestStat> lastStats = new ConcurrentHashMap<String, TestStat>();
		
		while (true) 
		{
			try 
			{
			      log.info("!!!!!!!!!!!!sleeptime is"+sleepTime);	
                              Thread.sleep(sleepTime);
			}
			catch (InterruptedException e) 
			{
				
			}
			if (runner != null) 
			{
				
				Map<String, TestStat> stats = runner.getStats();
				for (String name : stats.keySet()) 
				{
					TestStat lst = lastStats.get(name);
					if (lst == null) lst = new TestStat();
					
					TestStat cst = stats.get(name);
					TestStat delta = cst.deltaStat(lst);
					log.info("output stat now!");
					outputStat(name, delta);
				        log.info("output finished");	
					lastStats.put(name, lst);
				}
			}
		}
	}

	public void outputStat(String name, TestStat stat)
       {
	       if(stat==null)
                {
                     log.info("stat null");
                     return;
                }
	       if(stat.totalCount.get()>0)
		{
                     log.info(name+"=>"+stat+", tps: "+stat.succCount.floatValue()/sleepTime*1000);
		}
               else if(stat.totalCount.get()==0)
                {
                     log.info(name+"=>"+stat+", tps: 0");
                }
               else
                     return;
	}
	public void outputFinalStat(String name, TestStat stat) {
		
		 if (stat.totalCount.get() > 0)
		 {
			log.info(name+"=>"+stat);
			//System.out.println(name+"=>"+stat);
		}	
	}
}
