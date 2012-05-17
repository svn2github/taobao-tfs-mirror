package com.taobao.common.tfs.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.etao.core.storage.common.utils.Configuration;

public class ResourceCenterConfig extends Config {
	private int mysqlPort;
	private String mysqlAddr;
	private String user;
	private String password;
	private String tfsStatusDB;

	public void setMysqlPort(int port) {
		this.mysqlPort = port;
	}

	public void setMysqlAddr(String addr) {
		this.mysqlAddr = addr;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setTfsStatusDB(String tfsStatusDB) {
		this.tfsStatusDB = tfsStatusDB;
	}

	public int getMysqlPort() {
		return this.mysqlPort;
	}

	public String getMysqlAddr() {
		return this.mysqlAddr;
	}

	public String getUser() {
		return this.user;
	}

	public String getPassword() {
		return this.password;
	}

	public String getTfsStatusDB() {
		return this.tfsStatusDB;
	}

	@Override
	public void init() throws IOException {
		Configuration serverConf = getServerConfig();

		// update for public section
		String section = "public";
		Map<String, String> kvs = new HashMap<String, String>();
		kvs.put("port", getPort() + "");
		kvs.put("ip_addr", getIp());
		kvs.put("dev_name", getDev());
		kvs.put("work_dir", getWorkDir());
		serverConf.setValue(section, kvs);

		// update for rcserver section
		kvs.clear();
		kvs.put("rc_db_info", getMysqlAddr() + ":" + getMysqlPort() + ":"
				+ getTfsStatusDB());
		kvs.put("rc_db_user", getUser());
		kvs.put("rc_db_pwd", getPassword());
		serverConf.setValue(section, kvs);

		serverConf.save();
	}

}
