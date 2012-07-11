package com.taobao.common.tfs.utility.concurrent;

import com.taobao.common.tfs.DefaultTfsManager;

public class MetaSaveFileOperation implements Operation 
{
	private DefaultTfsManager tfsManager = null;
	private long appId = 0;
	private long userId = 0;
	private String localFile = null;
	private String metaFile = null;

	public Object run() 
	{
		return tfsManager.saveFile(appId, userId, localFile, metaFile);
	}

	public MetaSaveFileOperation(DefaultTfsManager tfsManager, long appId, long userId, String localFile, String metaFile) 
	{
		this.tfsManager = tfsManager;
		this.appId = appId;
		this.userId = userId;
		this.localFile = localFile;
		this.metaFile = metaFile;
	}

}
