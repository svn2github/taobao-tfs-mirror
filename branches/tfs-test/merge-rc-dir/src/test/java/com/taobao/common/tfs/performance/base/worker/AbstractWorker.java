package com.taobao.common.tfs.performance.base.worker;

import com.taobao.common.tfs.performance.base.TestStat;
import com.taobao.common.tfs.performance.base.WorkerConfig;

public abstract class AbstractWorker extends Thread {
	protected TestStat stat = new TestStat();
	public long tid =0;
	public boolean init(WorkerConfig config,long id) 
	{
		tid=id;
		return true;
		
	}
	
	public boolean shouldRun()
	{
		return true;
	}
	
	public void run() 
	{
		while (shouldRun()) 
		{
			boolean ret = before_work();

			long et = 0;

			if (ret) 
			{
				long st = System.nanoTime();
				ret = do_work();
				et = (System.nanoTime() - st) / 1000;
			}
			if (ret) 
			{
				ret = end_work();
			}

			// do stat
			stat.totalRt.addAndGet(et);

			if (et > stat.maxRt) {
				stat.maxRt = et;
			}

			stat.totalCount.incrementAndGet();
			if (ret) {
				stat.succCount.incrementAndGet();
			} else {
				stat.failCount.incrementAndGet();
			}
		}
	
	}

	public TestStat getStat() {
		return stat;
	}

	public boolean before_work() {
		return true;
	}

	public abstract boolean do_work();

	public boolean end_work() {
		return true;
	}

}
