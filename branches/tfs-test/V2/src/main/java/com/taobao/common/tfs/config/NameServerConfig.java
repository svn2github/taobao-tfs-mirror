package com.taobao.common.tfs.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.etao.core.storage.common.utils.Configuration;

public class NameServerConfig extends Config {
	
	//By default, the cluster id is 1
	private int clusterId = 1;
	
	private boolean masterServer = false;
	
	public void setMasterServer(boolean flag){
		masterServer = flag;
	}
	
	public boolean isMasterServer(){
		return masterServer;
	}
	
	public void setClusterId(int clusterId){
		this.clusterId = clusterId;
	}
	
	public int getClusterId(){
		return clusterId;
	}
	
	public String getVip(){
		return getMasterNsConfig().getIp();
	}
	
	public String getIpAddrList(){
		String ipList = getIp();
		
		for(Config each:getRefConfigs(NameServerConfig.class)){
				ipList = ipList+"|"+((NameServerConfig)each).getIp();
		}
		
		if(ipList.equals(ip)){
			ipList = ipList+"|"+ipList;
		}
		
		return ipList;
	}
	
	public void init() throws IOException{
		Configuration serverConf = getServerConfig();
		
		//update for public section
		String section = "public";
		Map<String,String> kvs = new HashMap<String,String>();
		kvs.put("port", getPort()+"");
		kvs.put("ip_addr", getVip());
		kvs.put("dev_name", getDev());
		kvs.put("work_dir", getWorkDir());
		serverConf.setValue(section, kvs);
		
		//update for nameserver section
		section = "nameserver";
		kvs.clear();
		kvs.put("cluster_id", getClusterId()+"");
		kvs.put("ip_addr_list",getIpAddrList());
		serverConf.setValue(section, kvs);
		
		serverConf.save();
	}
	
	public NameServerConfig getMasterNsConfig(){
		if(isMasterServer()){
			return this;
		}
		
		for(Config conf:getRefConfigs(NameServerConfig.class)){
			if(((NameServerConfig)conf).isMasterServer()){
				return (NameServerConfig)conf;
			}
		}
		
		return null;
	}
}
