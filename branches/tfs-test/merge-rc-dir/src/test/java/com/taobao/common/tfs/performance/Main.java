package com.taobao.common.tfs.performance;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.log4j.Logger;

import com.taobao.common.tfs.performance.base.TestConfig;
import com.taobao.common.tfs.performance.base.TestRunner;
import com.taobao.common.tfs.performance.base.WorkerConfig;


public class Main {

	/**
	 * @param args
	 */
	
	public static void main(String[] args) throws Exception
	{
		String confFileName = "confMix.xml";
		if (args.length > 0 ) 
		{
			confFileName = args[0];
		}
		InputStream in = new FileInputStream(confFileName);
		TestConfig config = TestConfig.getInstance();
		config.init(in);

		TestRunner runner = new TestRunner();
		HookThread hook = new HookThread();
		
		hook.setRunner(runner);
		Runtime.getRuntime().addShutdownHook(hook);
		runner.setStatInterval(config.getStatInterval());
		
		for (WorkerConfig conf : config.getWorkerConfig())
		{
			runner.addWorker(conf);
		}
		runner.start();
	}
	
	static class HookThread extends Thread 
	{
		TestRunner runner;
		public void setRunner(TestRunner runner) 
		{
			this.runner = runner;
		}

		@Override
		public void run() 
		{
			System.out.println("**************³ÌÐò½áÊø*****************");
			this.runner.dumpResult();
		}
	}

}
