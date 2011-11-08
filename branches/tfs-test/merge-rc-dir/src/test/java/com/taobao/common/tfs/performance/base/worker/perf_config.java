package com.taobao.common.tfs.performance.base.worker;

import org.dom4j.Element;
import com.taobao.common.tfs.performance.base.WorkerConfig;


public class perf_config extends WorkerConfig 
{
	public boolean isVerify;

	@Override
	public boolean parse(Element element) 
	{
		isVerify = Boolean.parseBoolean(element.attributeValue("isVerify"));
		return true;
	}
}
