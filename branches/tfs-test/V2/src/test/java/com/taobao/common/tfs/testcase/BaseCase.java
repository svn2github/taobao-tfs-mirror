package com.taobao.common.tfs.testcase;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.utility.FileUtility;

public class BaseCase {
	public static DefaultTfsManager tfsManager=null ;
	public static String resourcesPath="";
	
	public static int INVALID_MODE = 0;
	public static int R_MODE = 1;
	public static int RW_MODE = 2;
	public static int RC_VALID_MODE = 1;
	public static int RC_INVALID_MODE = 2;
	
    public static long appId;
    public static long userId=8;
    //public static String appKey = "testKeyYS";
    public static String appKey = "testKeyYS";
    protected final int MAX_UPDATE_TIME = 20;
    protected final int MAX_STAT_TIME = 40;
    
    protected List<String> tfsNames = new ArrayList<String>();
    protected List<String> metaFiles = new ArrayList<String>();
    
    private static ArrayList<String> testFileList = new ArrayList<String>();
    protected static Log log = LogFactory.getLog(BaseCase.class);

    static 
	{
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
        appId = tfsManager.getAppId();
        System.out.println("appid is : "+appId);
        //appKey = tfsManager.getAppKey();
    }
    
    public String currentDateTime(){
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmm");
    	return dateFormat.format(new Date());
    }
    
    public String getCurrentFunctionName(){
    	return Thread.currentThread().getStackTrace()[2].getMethodName();
    }
    
    public DefaultTfsManager createTfsManager() {
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		DefaultTfsManager tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
		appId = tfsManager.getAppId();

		return tfsManager;
	}

	@BeforeClass
	public static void beforeSetup() 
	{
		log.debug(" @@ beforeclass begin");
		
		Map<String,Integer> fileMap = new HashMap<String,Integer>(){
			private static final long serialVersionUID = 1L;

			{
				put("1B.jpg",1);
				put("2B.jpg",2);
				put("1K.jpg",1<<10);
				put("10K.jpg",10*(1<<10));
				put("100K.jpg",100*(1<<10));
				put("1M.jpg",1<<20);
				put("2M.jpg",2*(1<<20));
				put("3M.jpg",3*(1<<20));
				put("5M.jpg",5*(1<<20));
				put("10M.jpg",10*(1<<20));
				put("100M.jpg",100*(1<<20));
				put("1G.jpg",1<<30);
				//put("6G.jpg",6*(1<<30));
			}
		};
		
		for(String file:fileMap.keySet()){
			testFileList.add(file);
			FileUtility.createFile(file, fileMap.get(file));
		}
		
	}

	@AfterClass
	public static void afterTearDown() 
	{

		System.out.println(" @@ afterclass begin");
		int size = testFileList.size();
		for (int i = 0; i < size; i++) 
		{
			File file = new File(testFileList.get(i));
			System.out.println("file name: "+testFileList.get(i));
			if (file.exists()) 
			{
				file.delete();
			}
		}
	}
	
	@After
	public void tearDown(){
		if(tfsManager==null){
			return;
		}
		
		for (String each : tfsNames) {
			log.info("unlink file: " + each);
			tfsManager.unlinkFile(each, null);
		}
		
		for(String each:metaFiles){
			tfsManager.rmFile(appId, userId, each);
		}
	}
}
