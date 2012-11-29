package com.taobao.common.tfs.server;

import com.taobao.common.tfs.config.DataServerConfig;

public class DataServer extends DefaultTfsServer {
	private DataServerConfig configuration;
	
	public void setConfiguration(DataServerConfig configuration){
		this.configuration = configuration;
	}
	
	@Override
	public DataServerConfig getConfiguration() {
		return configuration;
	}
	
	

}
