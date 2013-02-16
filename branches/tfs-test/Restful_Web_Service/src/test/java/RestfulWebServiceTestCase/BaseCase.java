package RestfulWebServiceTestCase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;


import junit.framework.TestCase;

import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import Tool.AssertTool;
import Tool.ExpectMessage;
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
	protected static String App_key = Server.getTfs_app_key();
	protected static String App_id = "1";
	protected static String User_id = "727";
	
	static
	{
		ExecuteUrl.SetBase("http://"+Server.getRestful_web_server());
		ExecuteUrl.Init();
		FileMap.put("10K", 10*(1<<10));
		FileMap.put("1B", 1);
		FileMap.put("3M", 3*(1<<20));
		FileMap.put("2M", 2*(1<<20));
		FileMap.put("20M", 20*(1<<20));
		FileMap.put("30M", 30*(1<<20));
		FileMap.put("100M", 100*(1<<20));
		FileMap.put("1G", 1<<30);
		FileMap.put("empty", 0);
		//createFileMap(FileMap);
	}
	
    public  void deleteDir(String s,int n,String Seed)
    {
    	Map<String, String> Ret = new HashMap<String, String>();
    	ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
    	if(n==1)
    	{
    		Ret = RmDir(App_id,User_id,s.toString());
    		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
    	}
    	else
        {
    		n=n-1;
    		System.out.println(n);
    		deleteDir(s+"/"+Seed,n,Seed);
    		Ret = RmDir(App_id,User_id,s);
    		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
    		
    	}  
    }
    
    public  void deleteFile(String s,int n,String Seed)
    {
    	Map<String, String> Ret = new HashMap<String, String>();
    	ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
    	if(n==1)
    	{
    		Ret = RmFile(App_id,User_id,s.toString());
    		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
    	}
    	else
        {
    		n=n-1;
    		System.out.println(n);
    		deleteFile(s+"/"+Seed,n,Seed);
    		Ret = RmDir(App_id,User_id,s);
    		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
    		
    	}  
    }
    
	@Rule
	public TestWatcher watchman = new TestWatcher() 
	{
		protected String caseIdentifier = "";

		protected void starting(Description d) 
		{
			caseIdentifier = d.getClassName() + "." + d.getMethodName();
			System.out.println("\nstarting: " + caseIdentifier);
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
		if(null!=filePath)
		{
			File uploadFile = new File(filePath);
			try 
			{
				postMethod.setRequestEntity(new FileRequestEntity(uploadFile, null));
			} 
			catch (Exception ex) 
			{
				ex.printStackTrace();
			}
		}
		return postMethod;
	}
	
	protected PutMethod setPutMethod(String url, String filePath) 
	{
		PutMethod putMethod = new PutMethod(url);
		if(null!=filePath)
		{
			File uploadFile = new File(filePath);
			try 
			{
				putMethod.setRequestEntity(new FileRequestEntity(uploadFile, null));
			} 
			catch (Exception ex) 
			{
				ex.printStackTrace();
			}
		}
		return putMethod;
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
	
	protected HeadMethod setHeadMethod(String url) 
	{
		HeadMethod getMethod = new HeadMethod(url);
		return getMethod;
	}
	
	public Map<String, String> SaveFile(String file, String simple_name, String large_file,String suffix)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v1/"+App_key);
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
		
		ExecuteUrl.AddUrlDomain("v1/"+App_key);
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
		
		ExecuteUrl.AddUrlDomain("v1/"+App_key);
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
		
		ExecuteUrl.AddUrlDomain("v1/"+App_key);
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
	
	public Map<String, String> GetAppID(String App_Key)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_Key+"/appid");
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> CreateDir(String Appid, String Uid, String Dir, String recursive)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/dir/"+Dir);
		if(null!=recursive)
			ExecuteUrl.AddPara("recursive", recursive);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setPostMethod(ExecuteUrl.GetUrl(),null),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> RmDir(String Appid, String Uid, String Dir)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/dir/"+Dir);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setDeleteMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> MvDir(String Appid, String Uid, String SrcDir, String DestDir, String recursive)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/dir/"+DestDir);
		if(null!=recursive)
			ExecuteUrl.AddPara("recursive", recursive);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		PostMethod MvMethod = setPostMethod(ExecuteUrl.GetUrl(),null);
		MvMethod.setRequestHeader("x-tb-move-source","/"+SrcDir);
		Message = Tool.verifyResponse(MvMethod,null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> LsDir(String Appid, String Uid, String Dir)
	{
		Map<String, String> Message = new HashMap<String, String>();
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/dir/"+Dir);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> LsDirNeedAppKey(String Appkey, String Appid, String Uid, String Dir)
	{
		Map<String, String> Message = new HashMap<String, String>();
		ExecuteUrl.AddUrlDomain("v2/"+Appkey+"/"+Appid+"/"+Uid+"/dir/"+Dir);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> IsDirExist(String Appid, String Uid, String Dir)
	{
		Map<String, String> Message = new HashMap<String, String>();
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/dir/"+Dir);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setHeadMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	

	public Map<String, String> CreateFile(String Appid, String Uid, String File, String recursive)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/file/"+File);
		if(null!=recursive)
			ExecuteUrl.AddPara("recursive", recursive);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setPostMethod(ExecuteUrl.GetUrl(),null),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> RmFile(String Appid, String Uid, String File)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/file/"+File);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setDeleteMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> MvFile(String Appid, String Uid, String SrcFile, String DestFile, String recursive)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/file/"+DestFile);
		if(null!=recursive)
			ExecuteUrl.AddPara("recursive", recursive);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		PostMethod MvMethod = setPostMethod(ExecuteUrl.GetUrl(),null);
		MvMethod.setRequestHeader("x-tb-move-source","/"+SrcFile);
		Message = Tool.verifyResponse(MvMethod,null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> LsFile(String Appid, String Uid, String File)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/metadata/"+Appid+"/"+Uid+"/file/"+File);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> LsFileNeedAppKey(String Appkey, String Appid, String Uid, String File)
	{
		Map<String, String> Message = new HashMap<String, String>();
		
		ExecuteUrl.AddUrlDomain("v2/"+Appkey+"/metadata/"+Appid+"/"+Uid+"/file/"+File);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> IsFileExist(String Appid, String Uid, String File)
	{
		Map<String, String> Message = new HashMap<String, String>();
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/file/"+File);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setHeadMethod(ExecuteUrl.GetUrl()),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> WriteFile(String Appid, String Uid, String File, String LocalFile, String offset)
	{
		Map<String, String> Message = new HashMap<String, String>();
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/file/"+File);
		if(null!=offset)
			ExecuteUrl.AddPara("offset", offset);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setPutMethod(ExecuteUrl.GetUrl(),LocalFile),null);
		ExecuteUrl.ResetUrl();
		return Message;
	}
	
	public Map<String, String> ReadFile(String Appid, String Uid, String File, String LocalFile, String size, String offset)
	{
		Map<String, String> Message = new HashMap<String, String>();
		ExecuteUrl.AddUrlDomain("v2/"+App_key+"/"+Appid+"/"+Uid+"/file/"+File);
		if(null!=offset)
			ExecuteUrl.AddPara("offset", offset);
		if(null!=size)
			ExecuteUrl.AddPara("size", size);
		log.info(ExecuteUrl.GetUrl());
		HttpVerifyTool Tool = new HttpVerifyTool();
		Message = Tool.verifyResponse(setGetMethod(ExecuteUrl.GetUrl()),LocalFile);
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
			int writingSize = 0;
			long writedSize = 0;
			int bufSize = 10*(1<<20);
			byte[] b = new byte[10*(1<<20)];
			while(writedSize!=fileSize)
			{
				random.nextBytes(b);
				if(bufSize<fileSize-writedSize)
				   writingSize = bufSize;
				else
				   writingSize=(int)(fileSize-writedSize);
					output.write(b, 0, writingSize);
					writedSize+=writingSize;
			}
			output.close();
			log.info("Create file "+path+" success!");
		} 
		catch (IOException e) 
		{
				//TODO Auto-generated catch block
				e.printStackTrace();
		}
		return 0;
	}
	
	static public void createFileMap(Map<String,Integer> FileMap)
	{
		Set<String> keySet = FileMap.keySet();
		Iterator<String> it = keySet.iterator();
		while(it.hasNext())
		{
			String key = it.next();
			createFile(key,FileMap.get(key));
		}
	}
	
	static protected void deleteFile(String FilePath)
	{
		File file = new File(FilePath);
		System.out.println("Delete file "+FilePath);
		file.delete();
	}
	//@AfterClass
	static public void cleanResource()
	{
		Set<String> keySet = FileMap.keySet();
		Iterator<String> it = keySet.iterator();
		while(it.hasNext())
		{
			String key = it.next();
			deleteFile(key);
		}
	}
}
