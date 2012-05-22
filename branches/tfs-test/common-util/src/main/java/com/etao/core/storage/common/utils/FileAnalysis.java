package com.etao.core.storage.common.utils;

import org.apache.log4j.Logger;

import com.etao.core.storage.common.handler.ProcessHandlerWrap;
import com.etao.gaia.exception.OperationException;
import com.etao.gaia.handler.OperationResult;
import com.etao.gaia.handler.ProcessHandler;

public class FileAnalysis {
	private static Logger logger = Logger.getLogger(FileAnalysis.class);
	private static ProcessHandler process = ProcessHandlerWrap.getProcessHandler();
	
	/**
	 * Get the keyword occurrence times in one file
	 * 
	 * @param server - the server where the file locates
	 * @param filePath - file path
	 * @param keyword
	 * 
	 * @return return -1 if error occurs
	 */
	public static int getKeywordCount(String server,String filePath,String keyword){
		int times = 0;
		String findCmd = "cat " + filePath + " | grep -P " + "'" + keyword
				+ "'";

		OperationResult result = null;
		try {
			result = process.executeCmd(server, findCmd, false);

			if (result.getReturnCode() != 0) {
				logger.debug("The key word(\"" + keyword
						+ "\") is failed to find! the rc: "
						+ result.getReturnCode());
				times = -1;
			} else {

				String content = result.getMsg();
				times = content.length();
				logger.debug("The key word(\"" + keyword
						+ "\") has been found " + times + " times.");

			}
		} catch (OperationException e) {
			logger.error("Execute cmd exception: "+e.getMessage());
			times = -1;
		}

		return times;
	}
}
