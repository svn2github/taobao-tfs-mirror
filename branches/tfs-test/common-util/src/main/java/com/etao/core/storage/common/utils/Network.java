package com.etao.core.storage.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.log4j.Logger;

import com.etao.core.storage.common.handler.ProcessHandlerWrap;
import com.etao.gaia.exception.OperationException;
import com.etao.gaia.handler.OperationResult;
import com.etao.gaia.handler.ProcessHandler;

public class Network {
	private static Logger logger = Logger.getLogger(Network.class);
	private static ProcessHandler process = ProcessHandlerWrap
			.getProcessHandler();
	
	/**
	 * block the network between two group of servers
	 * 
	 * @param sources a list of source servers
	 * @param targets a list of target servers
	 */
	public static boolean blockServers(List<String> sources,
			List<String> targets) {
		logger.debug("begin blockServers");

		for (String source : sources) {
			for (String target : targets) {
				if (!blockServer(source, target)) {
					return false;
				}
			}
		}

		logger.debug("end blockServers");
		return true;
	}

	private static boolean blockServer(String source, String target) {
		logger.debug("begin blockServer between " + source + " " + target);

		String blockCmd = "/sbin/iptables -A OUTPUT -d " + target
				+ " -j DROP;/sbin/iptables-save";

		OperationResult result = null;
		try {
			result = process.executeCmd(source, blockCmd, false);
		} catch (OperationException e) {
			logger.error("block network between " + source + " and " + target
					+ " exception");
			logger.error(e.getMessage());
			return false;

		}

		if (result.isSuccess()) {
			logger.debug("Network from " + source + " to " + target
					+ " is blocked");
			return true;
		} else {
			logger.error("Network from " + source + " to " + target
					+ " is failed to block");
			logger.error("STAF msg: " + result.getMsg());
			return false;
		}

	}
	
	/**
	 * free the network between two group of servers
	 * 
	 * @param sources a list of source servers
	 * @param targets a list of target servers
	 */
	public static boolean freeServers(List<String> sources, List<String> targets) {
		logger.debug("begin freeServers");

		for (String source : sources) {
			for (String target : targets) {
				if (!freeServer(source, target)) {
					return false;
				}
			}
		}

		logger.debug("end freeServers");
		return true;
	}

	private static boolean freeServer(String source, String target) {
		logger.debug("begin freeServer between " + source + " " + target);

		String freeCmd = "/sbin/iptables -D OUTPUT -d " + target
				+ " -j DROP;/sbin/iptables-save";

		OperationResult result = null;
		try {
			result = process.executeCmd(source, freeCmd, false);
		} catch (OperationException e) {
			logger.error("free network between " + source + " and " + target
					+ " exception");
			logger.error(e.getMessage());
			return false;

		}

		if (result.isSuccess()) {
			logger.debug("Network from " + source + " to " + target
					+ " is freeed");
			return true;
		} else {
			logger.error("Network from " + source + " to " + target
					+ " is failed to free");
			logger.error("STAF msg: " + result.getMsg());
			return false;
		}

	}
	
	/**
	 * get the local ip address
	 * 
	 */
	public static String getLocalIPAddr() {
		InetAddress inet;
		try {
			inet = InetAddress.getLocalHost();
			return inet.getHostAddress();
		} catch (UnknownHostException e) {
			logger.error("get local ip error: " + e.getMessage());
			return null;
		}
	}

}
