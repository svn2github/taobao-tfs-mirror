package com.etao.core.storage.common.utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import com.etao.core.storage.common.handler.FileHandlerWrap;
import com.etao.gaia.handler.FileHandler;
import com.etao.gaia.handler.OperationResult;

public class Configuration {
	private String ip;
	private String confName;
	private String localFileName;

	private ConfigParser confParser;
	private Logger logger = Logger.getLogger(Configuration.class);
	private static FileHandler fileHandler = FileHandlerWrap.getFileHandler();
	
	
	public Configuration(String ip, String confName)
			throws IOException {
		this.ip = ip;
		this.confName = confName;
		this.localFileName = getLocalFileName(ip, confName);
		this.confParser = getConfParser(ip, confName);
	}
	
	/**
	 * save modified configuration file to remote server
	 * 
	 */
	public boolean save() {
		OperationResult result = fileHandler.copyRenameFile(
				Network.getLocalIPAddr(), localFileName, ip, confName);
		logger.debug("save file, ret code= " + result.getReturnCode()
				+ "; msg=" + result.getMsg());
		return result.isSuccess();
	}
	
	/**
	 * get configuration value specified by key in section
	 * 
	 * @param section
	 * @param key
	 * @return return null if key not found
	 */
	public String getValue(String section, String key) {
		return confParser.getValue(section, key);
	}
	
	/**
	 * set configuration values in section
	 * 
	 * @param section
	 * @param kvMaps map that stores key/value pairs
	 */
	public boolean setValue(String section, Map<String, String> kvMaps) {
		return confParser.setValue(section, kvMaps);
	}
	
	/**
	 * comment one configuration line which contains the keyword in section
	 * If multiple lines match, only the first line is commented
	 * 
	 * @param section
	 * @param keyword the keyword appears in the configuration line
	 */
	public boolean commentConfItem(String section, String keyword) {
		return confParser.commentConfItem(section, keyword);
	}

	/**
	 * uncomment one configuration line which contains the keyword in section
	 * If multiple lines match, only the first line is uncommented
	 * 
	 * @param section 
	 * @param keyword the keyword appears in the configuration line
	 */
	public boolean unCommentConfItem(String section, String keyword) {
		return confParser.unCommentConfItem(section, keyword);
	}

	private String getLocalFileName(String ip, String confName) {
		return getWorkingDir() + "/conf/" + ip + "_" + getFileName(confName);
	}

	private ConfigParser getConfParser(String ip, String confName)
			throws IOException {
		ConfigParser result = null;
		String localFileName = getLocalFileName(ip, confName);

		if (fileHandler.isEntryExisted(ip, confName)) {
			OperationResult opResult = fileHandler.copyRenameFile(ip, confName,
					Network.getLocalIPAddr(), localFileName);
			logger.debug("get file rc=" + opResult.getReturnCode() + "; msg="
					+ opResult.getMsg());

			if (opResult.isSuccess()&&new File(localFileName).exists()) {
				result = new ConfigParser(localFileName);
			}else{
				throw new IOException();
			}
		} else {
			throw new IOException();
		}

		return result;
	}

	private String getFileName(String path) {
		String separator = "";

		if (path.contains("/")) {
			separator = "/";
		} else if (path.contains("\\")) {
			separator = "\\";
		} else {
			return path;
		}

		return path.substring(path.lastIndexOf(separator) + 1);
	}

	private static String getWorkingDir() {
		return System.getProperty("user.dir").trim();
	}

}
