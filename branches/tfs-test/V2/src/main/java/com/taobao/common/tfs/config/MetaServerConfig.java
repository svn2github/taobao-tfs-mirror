package com.taobao.common.tfs.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.etao.core.storage.common.utils.Configuration;

public class MetaServerConfig extends Config {
	private int mysqlPort;
	private String mysqlAddr;
	private String user;
	private String password;
	private String metaDB;

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

	public void setMetaDB(String metaDB) {
		this.metaDB = metaDB;
	}

	public String getMetaDB() {
		return this.metaDB;
	}

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

		// update for metaserver section
		section = "metaserver";
		RootServerConfig rsConfig = (RootServerConfig) getRefConfigs(
				RootServerConfig.class).get(0);
		kvs.clear();
		kvs.put("ip_addr", rsConfig.getIp() + ":" + rsConfig.getPort());
		kvs.put("meta_db_infos", getMysqlAddr() + ":" + getMysqlPort() + ":"
				+ getMetaDB() + "," + getUser() + "," + getPassword()+";");
		
		serverConf.setValue(section, kvs);
		
		serverConf.save();
	}

}
