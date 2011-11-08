package com.taobao.common.tfs.performance.base;

import java.util.concurrent.atomic.AtomicLong;

public class TestStat {

	public AtomicLong totalCount = new AtomicLong();
	public AtomicLong succCount = new AtomicLong();
	public AtomicLong failCount = new AtomicLong();
	public AtomicLong totalRt = new AtomicLong();
	public long maxRt = 0;
	
	public void addStat(TestStat stat) {
		stat.totalCount.addAndGet(totalCount.get());
		stat.succCount.addAndGet(succCount.get());
		stat.failCount.addAndGet(failCount.get());
		stat.totalRt.addAndGet(totalRt.get());
		if (maxRt > stat.maxRt) {
			stat.maxRt = maxRt;
		}
	}
	
	public TestStat deltaStat(TestStat stat) {
		TestStat delta = new TestStat();
		delta.totalCount.set(totalCount.get() - stat.totalCount.get());
		delta.succCount.set(succCount.get() - stat.succCount.get());
		delta.failCount.set(failCount.get() - stat.failCount.get());
		delta.totalRt.set(totalRt.get() - stat.totalRt.get());
		
		long max = (maxRt > stat.maxRt) ? maxRt : stat.maxRt;
		delta.maxRt = max;
		
		stat.totalCount.set(totalCount.get());
		stat.succCount.set(succCount.get());
		stat.failCount.set(failCount.get());
		stat.totalRt.set(totalRt.get());
		stat.maxRt = max;
		
		return delta;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("total count: " + totalCount.get() + ", ");
		sb.append("succCount: " + succCount.get() + ", ");
		sb.append("failCount: " + failCount.get() + ", ");
		sb.append("succPect: " + (succCount.get()*100.0/totalCount.get()) + "%, ");
		if(totalCount.get()!=0)
                {
                   sb.append("avg rt: " + (totalRt.get()/totalCount.get()) + "(us), ");
                }
                else
                   sb.append("avg rt: larger than "+ totalRt.get() + "(us), ");
		sb.append("max rt: " + maxRt + "(us)");
		return sb.toString();
	}
}
