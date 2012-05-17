package com.taobao.common.tfs.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.etao.core.storage.common.utils.Configuration;

public class DataServerConfig extends Config {
	private String mountName = "";
	private String slaveNsVip = "";

	public void setMountName(String mountName) {
		this.mountName = mountName;
	}

	public String getMountName() {
		return this.mountName;
	}

	public void setSlaveNsVip(String slaveNsVip) {
		this.slaveNsVip = slaveNsVip;
	}

	public String getSlaveNsVip() {
		return this.slaveNsVip;
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

		// update for dataserver section
		section = "dataserver";
		RootServerConfig rsConfig = (RootServerConfig) getRefConfigs(
				RootServerConfig.class).get(0);

		kvs.clear();
		kvs.put("ip_addr", rsConfig.getVip());
		kvs.put("ip_addr_list", rsConfig.getIpAddrList());
		kvs.put("port", rsConfig.getMasterNsConfig().getPort()+"");
		kvs.put("mount_name", "");
		
		if(!"".equals(getSlaveNsVip())){
			kvs.put("slave_nsip", this.getSlaveNsVip());
		}
		
		serverConf.setValue(section, kvs);
		
		serverConf.save();
	}

	@Override
	public String getVip() {
		return getIp();
	}

}
