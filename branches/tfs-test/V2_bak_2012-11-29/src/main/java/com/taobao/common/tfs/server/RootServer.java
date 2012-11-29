package com.taobao.common.tfs.server;

import com.taobao.common.tfs.config.RootServerConfig;

public class RootServer extends DefaultTfsServer {
	private RootServerConfig configuration;
	
	public void setConfiguration(RootServerConfig configuration){
		this.configuration = configuration;
	}
	
	@Override
	public RootServerConfig getConfiguration() {
		return configuration;
	}

}
