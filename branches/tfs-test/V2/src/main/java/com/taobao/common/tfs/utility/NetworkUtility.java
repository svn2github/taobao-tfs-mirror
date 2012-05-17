package com.taobao.common.tfs.utility;

import org.apache.log4j.Logger;
import com.etao.gaia.exception.OperationException;
import com.etao.gaia.handler.OperationResult;
import com.etao.gaia.handler.ProcessHandler;
import com.etao.gaia.handler.staf.ProcessHandlerSTAFImpl;

public class NetworkUtility {
	private static Logger logger = Logger.getLogger(NetworkUtility.class);
	private static ProcessHandler processhandler = ProcessHandlerSTAFImpl
			.getProcessHandler();

	public static boolean freePort(String tarIp, String freeIp, int port) {
		boolean ret = false;
		String cmd = "/sbin/iptables -D OUTPUT -p tcp -d " + freeIp
				+ " --dport " + port + " -j DROP;/sbin/iptables-save";

		OperationResult result = null;

		try {
			result = processhandler.executeCmd(tarIp, cmd, false);
		} catch (OperationException e) {
			e.printStackTrace();
		}

		if (result.isSuccess()) {
			logger.debug("Network from " + tarIp + " to " + freeIp
					+ " is freed on port " + port);
			ret = true;
		} else {
			logger.error("Network from " + tarIp + " to " + freeIp
					+ " is failed to be freed on port " + port);
			ret = false;
		}

		return ret;
	}

	public static boolean blockPort(String tarIp, String blkIp, int port) {
		boolean ret = false;
		String cmd = "/sbin/iptables -A OUTPUT -p tcp -d " + blkIp
				+ " --dport " + port + " -j DROP;/sbin/iptables-save";

		OperationResult result = null;

		try {
			result = processhandler.executeCmd(tarIp, cmd, false);
		} catch (OperationException e) {
			e.printStackTrace();
		}

		if (result.isSuccess()) {
			logger.debug("Network from " + tarIp + " to " + blkIp
					+ " is blocked on port " + port);
			ret = true;
		} else {
			logger.error("Network from " + tarIp + " to " + blkIp
					+ " is failed to be blocked on port " + port);
			ret = false;
		}

		return ret;
	}

	public static boolean blockNetwork(String tarIp, String blkIp) {

		boolean bRet = false;
		String strCmd = "/sbin/iptables -A OUTPUT -d " + blkIp
				+ " -j DROP;/sbin/iptables-save";

		OperationResult result = null;
		try {
			result = processhandler.executeCmd(tarIp, strCmd, false);
		} catch (OperationException e) {
			e.printStackTrace();
		}

		if (result.isSuccess()) {
			logger.debug("Network from " + tarIp + " to " + blkIp
					+ " is blocked!");
			bRet = true;
		} else {
			logger.error("Network from " + tarIp + " to " + blkIp
					+ " is failed to block!!!");
			bRet = false;
		}

		return bRet;
	}

	public static boolean freeNetwork(String tarIp) {
		boolean bRet = false;
		String strCmd = "/sbin/iptables -F";

		OperationResult result = null;
		try {
			result = processhandler.executeCmd(tarIp, strCmd, false);
		} catch (OperationException e) {
			e.printStackTrace();
		}

		if (result.isSuccess()) {
			logger.debug("Network on " + tarIp + " is unblocked.");
			bRet = true;
		} else {
			logger.error("Network on " + tarIp + " is failed to unblock!!!");
			bRet = false;
		}

		return bRet;
	}
	
	//@Test
	public void getPid() throws OperationException{
		int i = processhandler.getPidByProcName("10.232.128.50", "/usr/local/staf/bin/STAFProc");
		System.out.println("proc id is: "+i);
		//OperationResult result = processhandler.executeCmd("10.232.36.206", "su - admin -c \"touch /home/admin/123\"", true);
		//System.out.println(result.getMsg());
	}
	
}
