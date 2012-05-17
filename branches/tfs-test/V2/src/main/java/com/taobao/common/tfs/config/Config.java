package com.taobao.common.tfs.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.etao.core.storage.common.utils.Configuration;

public abstract class Config {
	
	protected int port;
	protected String ip;
	protected String vip;
	protected String dev;
	protected String workDir;
	protected String startCmd;
	protected String configPath;
	protected List<Config> refConfigs = new ArrayList<Config>();
	
	protected boolean masterCluster = false;
	
	//used for save configuration to server
	private Configuration serverConfig;
	
	public void setMasterCluster(boolean flag){
		this.masterCluster = flag;
	}
	
	public boolean isMasterCluster(){
		return masterCluster;
	}
	
	public void setPort(int port){
		this.port = port;
	}
	
	public int getPort(){
		return this.port;
	}
	
	public void setIp(String ip){
		this.ip = ip;
	}
	
	public String getIp(){
		return this.ip;
	}
	
	public void setDev(String dev){
		this.dev = dev;
	}
	
	public String getDev(){
		return this.dev;
	}
	
	public void setConfigPath(String configPath){
		this.configPath = configPath;
	}
	
	public String getConfigPath(){
		return this.configPath;
	}
	
	public void setWorkDir(String workDir){
		this.workDir = workDir;
	}
	
	public String getWorkDir(){
		return this.workDir;
	}
	
	public void setStartCmd(String startCmd){
		this.startCmd = startCmd;
	}
	
	public String getStartCmd(){
		return this.startCmd;
	}
	
	public void addRefConfig(Config conf){
		refConfigs.add(conf);
	}
	
	public void clearRefConfigs(){
		refConfigs.clear();
	}
	
	public List<Config> getRefConfigs(){
		return refConfigs;
	}
	
	public List<Config> getRefConfigs(Class <?extends Config> type){
		List<Config> result = new ArrayList<Config>();
		
		for(Config each:refConfigs){
			if(each.getClass()==type){
				result.add(each);
			}
		}
		
		return result;
	}
	
	public Configuration getServerConfig() throws IOException{
		if(serverConfig==null){
			serverConfig = new Configuration(ip,configPath);
		}
		
		return serverConfig;
	}
	
	
	public abstract void init() throws IOException;
	
}
