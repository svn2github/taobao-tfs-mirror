package com.taobao.common.tfs.performance.base;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class TestConfig {

	private static TestConfig configInstace;

	public static TestConfig getInstance()
	{
		if (configInstace == null) 
		{
			configInstace = new TestConfig();
		}
		return configInstace;
	}

	long statInterval;

	private Document document = null;
	
	private String address;
	private int port;
	private int iTimeout;

	List<WorkerConfig> workerconfigs = new ArrayList<WorkerConfig>();

	public long getStatInterval() 
	{
		return statInterval;
	}

	public List<WorkerConfig> getWorkerConfig()
	{
		return workerconfigs;
	}

	public String getAddress() 
	{
		return address;
	}

	public int getPort() 
	{
		return port;
	}
	
	public int getTimeout()
	{
		return iTimeout;
	}

	public void init(String confFileName) throws Exception
	{
		SAXReader reader = new SAXReader();
		document = reader.read(new File(confFileName));
		parseDocument();
	}
	
	public void init(InputStream in) throws Exception 
	{
		SAXReader reader = new SAXReader();
		document = reader.read(in);
		parseDocument();
	}

	@SuppressWarnings("rawtypes")
	private void parseDocument() throws Exception 
	{
		checkRoot();
		Element root = document.getRootElement();
		for (Iterator it = root.elementIterator(); it.hasNext();)
		{
			Element element = (Element) it.next();
			if (element.getName() == "interval") 
			{
				statInterval = Long.parseLong(element.getStringValue());
			} 
			else if (element.getName() == "workers") 
			{
				parseWorker(element);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void parseWorker(Element workers) 
	{
		for (Iterator it = workers.elementIterator(); it.hasNext();) 
		{
			Element ele = (Element) it.next();
			if (ele.getName() == "server") 
			{
				address = ele.attributeValue("address");
				port = Integer.parseInt(ele.attributeValue("port"));
				iTimeout = Integer.parseInt(ele.attributeValue("timeout"));
				continue;
			} 
			else if (ele.getName() == "worker") 
			{
				String workerClassName = ele.attributeValue("configClassName");
				WorkerConfig wc = null;
				if (Utility.isNotEmpty(workerClassName)) 
				{
					wc = (WorkerConfig) Utility.newInstance(workerClassName);
				}
				else 
				{
					wc = new WorkerConfig();
				}
				
				wc.init(ele);	
				
				if (wc.threadCount > 0) 
				{
					workerconfigs.add(wc);
				}
			}
		}
	}

	private void checkRoot() throws Exception
	{
		Element root = document.getRootElement();
		if (root.getName() != "config") 
		{
			throw new Exception("config file root must be <config>");
		}

	}

	public static void main(String[] args) throws Exception
	{
		TestConfig config = new TestConfig();
		config.init("confMix.xml");
	}
}
