package com.taobao.common.tfs.performance.base;

import org.dom4j.Element;

public class WorkerConfig {
	public String name;
	public String className;
	public int threadCount;
		
	public boolean init(Element configElement) {
		name = configElement.attributeValue("name");
		className = configElement.attributeValue("className");
		threadCount = Integer.parseInt(configElement.attributeValue("threadCount"));
		return parse(configElement);
	}

	public boolean parse(Element element) {
		return true;
	}
}
