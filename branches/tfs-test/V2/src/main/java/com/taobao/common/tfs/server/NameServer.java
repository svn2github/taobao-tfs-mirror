package com.taobao.common.tfs.server;

import com.taobao.common.tfs.config.NameServerConfig;

public class NameServer extends DefaultTfsServer {
	private NameServerConfig configuration;
	
	public void setConfiguration(NameServerConfig configuration){
		this.configuration = configuration;
	}
	
	@Override
	public NameServerConfig getConfiguration() {
		return configuration;
	}

}
