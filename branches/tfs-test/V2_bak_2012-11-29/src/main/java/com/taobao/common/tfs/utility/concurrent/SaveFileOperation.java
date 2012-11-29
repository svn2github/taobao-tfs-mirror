package com.taobao.common.tfs.utility.concurrent;

import com.taobao.common.tfs.DefaultTfsManager;

public class SaveFileOperation implements Operation {
	private String fileName = null;
	private String suffix = null;
	private DefaultTfsManager tfsManager = null;

	public SaveFileOperation(DefaultTfsManager tfsManager, String fileName) 
	{
		this.fileName = fileName;
		this.tfsManager = tfsManager;
	}

	public SaveFileOperation(DefaultTfsManager tfsManager, String fileName, String suffix) 
	{
		this.fileName = fileName;
		this.suffix = suffix;
		this.tfsManager = tfsManager;
	}

	public Object run() 
	{
		return tfsManager.saveFile(fileName, null, suffix);
	}

}
