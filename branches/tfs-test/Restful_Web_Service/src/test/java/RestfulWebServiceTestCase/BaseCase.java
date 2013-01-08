package RestfulWebServiceTestCase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;


import junit.framework.TestCase;

import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


import Tool.HttpVerifyTool;
import Tool.RestfulWebServer;
import Tool.UrlJoin;

public class BaseCase 
{
	static UrlJoin ExecuteUrl = new UrlJoin ();
	
	public static final Log log = LogFactory.getLog(TestCase.class);
	protected static final ApplicationContext Bean = new ClassPathXmlApplicationContext("conf.xml");
	protected static final RestfulWebServer Server = (RestfulWebServer) Bean.getBean("RestfulWebServer");
	protected static Map<String,Integer> FileMap = new HashMap<String ,Integer> ();
	
	static
	{
		ExecuteUrl.SetBase("http://"+Server.getRestful_web_server()+"/v1/"+Server.getTfs_app_key());
		ExecuteUrl.Init();
		FileMap.put("10K", 10*(1<<10));
		FileMap.put("2M", 10*(1<<10));
		FileMap.put("20M", 20*(1<<20));
		FileMap.put("30M", 0*(1<<20));
		FileMap.put("empty", 0);
		//createFile("10K",10*(1<<10));
	}
	
	@Rule
	public TestWatcher watchman = new TestWatcher() 
	{
		protected String caseIdentifier = "";

		protected void starting(Description d) 
		{
			caseIdentifier = d.getClassName() + "." + d.getMethodName();
			System.out.println("starting: " + caseIdentifier);
		}

		protected void succeeded(Description d) 
		{
			caseIdentifier = d.getClassName() + " " + d.getMethodName();
			System.out.println("succeeded: " + caseIdentifier);
		}

		protected void failed(Throwable e, Description d) 
		{
			caseIdentifier = d.getClassName() + " " + d.getMethodName();
			System.out.println("failed: " + caseIdentifier);
		}

		protected void finished(Description d) 
		{
			caseIdentifier = d.getClassName() + " " + d.getMethodName();
			System.out.println("finished: " + caseIdentifier);
		}

	};
	
	protected PostMethod setPostMethod(String url, String filePath) 
	{
		PostMethod postMethod = new PostMethod(url);
		File uploadFile = new File(filePath);

		try 
		{
			FileInputStream fileInputStream = new FileInputStream(uploadFile);
			InputStreamRequestEntity inputStreamRequestEntity = new InputStreamRequestEntity(fileInputStream);
			postMethod.setRequestEntity((RequestEntity) inputStreamRequestEntity);
		} 
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
		return postMethod;
	}
	
	protected DeleteMethod setDeleteMethod(String url) 
	{
		DeleteMethod deleteMethod = new DeleteMethod(url);

		return deleteMethod;
	}

	protected GetMethod setGetMethod(String url) 
	{
		GetMethod getMethod = new GetMethod(url);
		return getMethod;
	}
	
	public Map<String, String> SaveFile(String file, String simple_name, String large_file,String suffix)
	{
		Map<String, String> Message = new HashMap<String, String>();
		if(null!=simple_name)
			ExecuteUrl.AddPara("simple_name", simple_name);
		if(null!=large_file)
			ExecuteUrl.AddPara("large_file", large_file);
		if(null!=suffix)
			ExecuteUrl.AddPara("suffix", suffix);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setPostMethod(ExecuteUrl.GetUrl(),file),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> FetchFile(String TFS_name, String file, String suffix, String offset, String size)
	{
		Map<String, String> Message = new HashMap<String, String>();
		if(null==TFS_name||""==TFS_name)
			return Message;
		ExecuteUrl.AddUrlDomain(TFS_name);
		if(null!=suffix)
			ExecuteUrl.AddPara("suffix", suffix);
		if(null!=offset)
			ExecuteUrl.AddPara("offset", offset);
		if(null!=size)
			ExecuteUrl.AddPara("size", size);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),file);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> DeleteFile(String TFS_name, String suffix, String hide)
	{
		Map<String, String> Message = new HashMap<String, String>();
		if(null==TFS_name||""==TFS_name)
			return Message;
		ExecuteUrl.AddUrlDomain(TFS_name);
		if(null!=suffix)
			ExecuteUrl.AddPara("suffix", suffix);
		if(null!=hide)
			ExecuteUrl.AddPara("hide", hide);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setDeleteMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> StatFile(String TFS_name, String suffix, String type)
	{
		Map<String, String> Message = new HashMap<String, String>();
		if(null==TFS_name||""==TFS_name)
			return Message;
		ExecuteUrl.AddUrlDomain("metadata");
		ExecuteUrl.AddUrlDomain(TFS_name);
		if(null!=suffix)
			ExecuteUrl.AddPara("suffix", suffix);
		if(null!=type)
			ExecuteUrl.AddPara("type", type);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public static int createFile(String path, long fileSize)
	{
		try 
		{
			File f = new File(path);
			if(!f.exists())
				f.createNewFile();
			Random random = new Random();
			FileOutputStream  output = new FileOutputStream (f);
			int writeSize = 10*(1<<20);
			long allSize = 0;
			byte[] b ;
			while(allSize!=fileSize)
			{
				if(writeSize<fileSize-allSize)
					b = new byte[writeSize];
				
				else
				{
					b = new byte[(int)(fileSize-allSize)];
					allSize=fileSize;
				}
				random.nextBytes(b);
				output.write(b);
				
			}
			output.close();
			log.info("Create file "+path+" success!");
		} 
		catch (IOException e) 
		{
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
		return 0;
	}
	
	public void createFileMap(Map<String,Integer> FileMap)
	{
		Set<String> keySet = FileMap.keySet();
		Iterator<String> it = keySet.iterator();
		while(it.hasNext())
		{
			String key = it.next();
			createFile(key,FileMap.get(key));
		}
	
	}
}































