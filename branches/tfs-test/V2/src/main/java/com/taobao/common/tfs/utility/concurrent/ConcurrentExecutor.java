package com.taobao.common.tfs.utility.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.taobao.common.tfs.utility.TimeUtility;


public class ConcurrentExecutor {
	private int threadCount;
	private Operation operation;
	
	private ExecutorService es;
	private ExecutorCompletionService<Object> ecs;
	
	private Logger logger = Logger.getLogger(ConcurrentExecutor.class);
	
	public ConcurrentExecutor(Operation operation,int threadCount){
		this.operation = operation;
		this.threadCount = threadCount;
		
		es = Executors.newFixedThreadPool(threadCount);
		ecs = new ExecutorCompletionService<Object>(es);
	}
	
	public void run(){
		for(int i = 0;i<threadCount;i++){
			ecs.submit(new Callable<Object>(){
				public Object call() throws Exception {
					return operation.run();
				}	
			});
		}
		es.shutdown();
		
	}
	
	public void runWithDelay(int delayInSeconds){
		logger.info("begin sleep");
		TimeUtility.sleep(delayInSeconds);
		logger.info("end sleep");
		run();
	}
	
	public List<Object> waitForComplete(){
		List<Object> result = new ArrayList<Object>();
		
		for(int i = 0; i<threadCount;i++){
			try {
				logger.info("waitForCompletetion "+threadCount);
				result.add(ecs.take().get());
			} catch (Exception e) {
				logger.error("waitForCompletetion exception",e);
			}
		}
		
		return result;
	}
}
