package com.taobao.common.tfs.server;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.gaia.exception.OperationException;
import com.etao.gaia.handler.OperationResult;
import com.etao.gaia.handler.ProcessHandler;
import com.etao.gaia.handler.staf.ProcessHandlerSTAFImpl;
import com.etao.gaia.server.Server;
import com.taobao.common.tfs.config.Config;

public abstract class DefaultTfsServer implements Server {
	Config conf = getConfiguration();
	ProcessHandler proc = ProcessHandlerSTAFImpl.getProcessHandler();

	protected Log log = LogFactory.getLog(DefaultTfsServer.class);

	public int start() {
		OperationResult result = null;

		try {
			result = proc.executeCmd(conf.getIp(), conf.getStartCmd(), false);
			log.info("start server result, rc=" + result.getReturnCode()
					+ "; msg=" + result.getMsg());
			if (result.isSuccess()) {
				return 0;
			}
		} catch (OperationException e) {
			log.error("start server exception: " + e.getMessage());
		}
		return -1;
	}

	public void stop() {

		OperationResult result = proc.killProcess(conf.getIp(),
				conf.getStartCmd());
		log.info("start server result, rc=" + result.getReturnCode() + "; msg="
				+ result.getMsg());

		int pid = proc.getPidByProcName(conf.getIp(), conf.getStartCmd());
		if (pid > 0) {
			proc.killProcessNow(conf.getIp(), pid);
		}
	}

	public void restart() {
		stop();
		start();
	}

	public boolean isRunning() {
		int pid = proc.getPidByProcName(conf.getIp(), conf.getStartCmd());

		return pid > 0 ? true : false;
	}

	public String getName() {
		return "no name specified";
	}

	public String getIP() {
		return conf.getIp();
	}

	public String getVIP() {
		return null;
	}

	public int getPort() {
		return conf.getPort();
	}
	
	public boolean init(){
		boolean result = false;
		try {
			getConfiguration().init();
		} catch (IOException e) {
			e.printStackTrace();
			result = false;
		}
		
		return result;
	}

	abstract public Config getConfiguration();

}
