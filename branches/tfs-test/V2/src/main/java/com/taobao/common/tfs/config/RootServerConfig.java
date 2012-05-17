package com.taobao.common.tfs.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.etao.core.storage.common.utils.Configuration;

public class RootServerConfig extends Config {
	private boolean masterServer = false;

	public void setMasterServer(boolean flag) {
		masterServer = flag;
	}

	public boolean isMasterServer() {
		return masterServer;
	}

	@Override
	public void init() throws IOException {
		Configuration serverConf = getServerConfig();

		// update for public section
		String section = "public";
		Map<String, String> kvs = new HashMap<String, String>();
		kvs.put("port", getPort() + "");
		kvs.put("ip_addr", getVip());
		kvs.put("dev_name", getDev());
		kvs.put("work_dir", getWorkDir());
		serverConf.setValue(section, kvs);
		
		section = "rootserver";
		kvs.clear();
		kvs.put("ip_addr_list", getIpAddrList());
		serverConf.setValue(section, kvs);
		
		serverConf.save();
	}

	public RootServerConfig getMasterNsConfig() {
		if (isMasterServer()) {
			return this;
		}

		for (Config conf : getRefConfigs(RootServerConfig.class)) {
			if (((RootServerConfig) conf).isMasterServer()) {
				return (RootServerConfig) conf;
			}
		}

		return null;
	}

	public String getVip() {
		return getMasterNsConfig().getIp();
	}

	public String getIpAddrList() {
		String ipList = getIp();

		for (Config each : getRefConfigs(NameServerConfig.class)) {
			ipList = ipList + "|" + ((NameServerConfig) each).getIp();
		}

		if (ipList.equals(ip)) {
			ipList = ipList + "|" + ipList;
		}

		return ipList;
	}

}
