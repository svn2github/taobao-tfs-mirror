package com.taobao.common.tfs.performance.base;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.taobao.common.tfs.performance.base.worker.AbstractWorker;

public class TestRunner 
{

	private ConcurrentHashMap<String, List<AbstractWorker>> taskMap = new ConcurrentHashMap<String, List<AbstractWorker>>();
	private StatThread statThread;
	protected static Logger log = Logger.getLogger("tfs_test_runner");
	
	
	
	
	public TestRunner() 
	{
		statThread = new StatThread();
		statThread.setRunner(this);
	}

	public void setStatInterval(long interval) {
		statThread.setSleepTime(interval);
	}

	public void addWorker(WorkerConfig config) {
		List<AbstractWorker> tasks = taskMap.get(config.name);
		if (tasks == null) {
			tasks = new ArrayList<AbstractWorker>();
		}
		
		for (int i = 0; i < config.threadCount; i++) {
			if (config.threadCount < 1) continue;
			AbstractWorker worker = (AbstractWorker) Utility.newInstance(config.className);
			worker.init(config,(long)(i+1));
			tasks.add(worker);
		}
		
		taskMap.put(config.name, tasks);
	}

	public void start() {
		for (String name : taskMap.keySet()) {
			List<AbstractWorker> tasks = taskMap.get(name);
			if (tasks != null && tasks.size() > 0) {
				start_task(name, tasks);
			}
		}
		statThread.start();
	}

	public Map<String, TestStat> getStats() {
		Map<String, TestStat> statMap = new ConcurrentHashMap<String, TestStat>();

		for (String name : taskMap.keySet()) {
			TestStat stat = new TestStat();
			for (AbstractWorker worker : taskMap.get(name)) {
				worker.getStat().addStat(stat);
			}
			statMap.put(name, stat);
		}

		return statMap;
	}
	
	public void dumpResult() {
		Map<String, TestStat> statMap = getStats();
		
		log.info("========================");
		for (String name : taskMap.keySet()) {
			statThread.outputFinalStat(name, statMap.get(name));
		}
	}

	private void start_task(String name, List<AbstractWorker> workers) {
		log.info("start [" + name + "] tasks, size: "
				+ workers.size());
		for (AbstractWorker worker : workers) {
			worker.start();
		}
	}

}
