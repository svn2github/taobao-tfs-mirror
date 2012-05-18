package com.taobao.common.tfs.server;

import com.taobao.common.tfs.config.ResourceCenterConfig;

public class ResourceCenter extends DefaultTfsServer {
	private ResourceCenterConfig configuration;
	
	public void setConfiguration(ResourceCenterConfig configuration){
		this.configuration = configuration;
	}
	
	@Override
	public ResourceCenterConfig getConfiguration() {
		return configuration;
	}

}
