package com.taobao.common.tfs.server;

import com.taobao.common.tfs.config.Config;
import com.taobao.common.tfs.config.MetaServerConfig;

public class MetaServer extends DefaultTfsServer {
	private MetaServerConfig configuration;
	
	public void setConfiguration(MetaServerConfig configuration){
		this.configuration = configuration;
	}
	
	@Override
	public Config getConfiguration() {
		return configuration;
	}

}
