package com.taobao.common.tfs.testcase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;


public class rcTfsBaseCase extends BaseCase 
{
	//rc server 配置文件的读取 (config&server)
	//...
	protected static Log log = LogFactory.getLog(rcTfsBaseCase.class);
	
    static 
	{
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
    }
	
	// Define
	final public int RCINDEX = 0;//??
	final public int RCNSINDEX = 1;//??
	//mysql连接
	//...
	
	
	
	//Other
	public String caseName = "";
	final public int WAITTIME = 30;
	public int FETCH_OPER = 1;
	public int SAVE_OPER = 2;
	public int UNLINK_OPER = 4;

	//public DefaultTfsManager tfsManager = null; 后面改
	//public static SimpleDateFormat d = new SimpleDateFormat("yyyyMMddhhmm");
	//已经有了
	
	
	//通过其他方式得到
	/*
	public static String appKey = "foobarRcAA";
	public static String rcAddr = "10.232.36.206:6261";
	public static String rsAddr = "10.232.36.206:8766";
	public static String appIp = "10.13.88.118";
	public static long appId = 3;
	public static long userId = 7;
	*/
	
	public static int MAX_UPDATE_TIME = 20;
	public static int MAX_STAT_TIME = 40;
	public static int INVALID_MODE = 0;
	public static int R_MODE = 1;
	public static int RW_MODE = 2;
	public static int RC_VALID_MODE = 1;
	public static int RC_INVALID_MODE = 2;

	public static int WITHOUT_DUPLICATE = 0;
	public static int WITH_DUPLICATE = 1;
	public static int RC_DEFAULT_INDEX = 0;
	public static int RC_NEW_INDEX = 1;
	public static int RC_INVALID_INDEX = 2;
	
	//基类有了
	/*
	public static String retLocalFile = "test_ret_file";
	public static ArrayList<String> testFileList = new ArrayList<String>();
	*/
	
	/*___________________________________函数区____________________________________*/
	
//	public DefaultTfsManager CreateTfsManager() 
//	{
//		DefaultTfsManager dtTfsManager;
//		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
//		dtTfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
//		appId = dtTfsManager.getAppId();
//		return dtTfsManager;
//	}
	
	
	
	
	
	
	
	
	
	
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	/*_____________________________________________________________________*/
	
//	
//	/*server 控制函数*/
//	public boolean killOneRc(int index);
//	public boolean startOneRc(int index);
//	/*Rc 数据库(包括权限)设置和查看函数*/
//	public long getCurrentQuote(String appKey);
//	public long getMaxQuote(String appKey);
//	public void resetCurQuote(String appKey);
//	public void modifyMaxQuote(String appKey, long newQuote);
//	public void setGroupPermission(String appKey, int mode);
//	public void setClusterPermission(String appKey, int mode);
//	public void setClusterPermission(String appKey, int index, int mode);
//	public int getClusterEx(String nsAddr);
//	public int getClusterIndex(String tfsname);
//	public boolean checkSessionId(String sessionId);//错开的
//	public boolean setDuplicateStatus(String appKey, int status);
//	public boolean changeDuplicateServerAddr(int clusterRackID,String duplicateServerAddr);
//	public long getUsedCapacity(String appKey);
//	public long getFileCount(String appKey);
//	public long getOperTimes(String sessionId, int operType);
//	public long getSuccTimes(String sessionId, int operType);
//	public long getFileSize(String sessionId, int operType);
//	
//	
//	
//	
//	/*Rc 机器集群控制*/
//	public void addNewTfsCluster(String appKey, int index, String clusterName);
//	public void removeTfsCluster(String appKey, int index);
//	public void addOneRc(int index, int status);
//	public void removeOneRc(int index);
//	public void modifyRc(int oldIndex, int newIndex);
//	public void setRcMode(int status);
//	public void setRcMode(int index, int status);
		
}