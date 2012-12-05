package com.taobao.common.tfs.testcase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.TfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.utility.MySQLConnector;
import com.taobao.common.tfs.utility.NameMetaDirHelper;



public class metaTfsBaseCase extends BaseCase 
{
	//meta client&server 配置文件的读取 (config&server)
	//慢慢改
	//...
	protected static Log log = LogFactory.getLog(metaTfsBaseCase.class);
	protected static MySQLConnector tfsSqlConnector = new MySQLConnector();
    public static long appId;
    public static String appKey;
    public static String rcAddr;
    public static long userId=8;
    public static String appIp;
    public static DefaultTfsManager tfsManagerM=null;
    
    //功能相关测试参量
    public static int MAX_STAT_TIME = 40;
    
	//MySql 配置
	protected static String server = "10.232.4.6:3306";
	protected static String db = "tfs_stat_db";
	protected static String user = "root";
	protected static String pwd = "123";
	protected static Connection tfsSqlCon;
    
    static 
	{
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
        appId = tfsManager.getAppId();
        appKey = tfsManager.getAppKey();
        System.out.println(appKey);
        rcAddr = tfsManager.getRcAddr();
        //tfsSqlCon = tfsSqlConnector.getConnection(server, db, user, pwd);
        
    }
    
    //测试辅助函数
    public static void deleteDir(String s,int n,TfsManager tfsManager)
    {
    	if(n==0)
    		tfsManager.rmDir(appId,userId,s);
    	else
        {
    		s="/text"+s;
    		n=n-1;
    		deleteDir(s,n,tfsManager);
    		tfsManager.rmDir(appId,userId,s);
    	}  
    }
    
    public  DefaultTfsManager createtfsManager()
    {
    	boolean bRet;
    	DefaultTfsManager tfsManagerMeta = new DefaultTfsManager();
    	tfsManagerMeta.setRcAddr(rcAddr);
    	tfsManagerMeta.setAppKey(appKey);
    	tfsManagerMeta.setAppIp(appIp);
    	tfsManagerMeta.setUseNameMeta(true);
        bRet = tfsManagerMeta.init();
        Assert.assertTrue(bRet);
        return tfsManagerMeta;
    }
    
    
    //MetaServer 控制函数
	public boolean killOneMeta(String Ip,String Path) 
	{
		boolean bRet = false;
		bRet=killOneServer(Ip,Path);
		return bRet;
	}
	public boolean startOneMeta(String Ip,String Path) 
	{
		boolean bRet = false;
		bRet=startOneServer(Ip,Path);
		return bRet;
	}
	
    //测试辅助函数MySql相关
	public boolean clearDB(ArrayList<String>serverList,ArrayList<String>pathList) 
	{
		long iRet = -1;
		boolean bRet = false;
		boolean tempRet1 = false;
		boolean tempRet2 = false;
		ResultSet result; 
		String SqlState = "delete from t_meta_info;";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		if(bRet)
		{
			if (serverList.size()!=pathList.size())
				{
					System.out.println("The num of server is not same to the path !!!");
					return false;
				}
			else
			{
				int num = serverList.size();
				for (int i=0;i<num;i++)
				{
					tempRet1=killOneMeta(serverList.get(i),pathList.get(i));
					tempRet2=startOneMeta(serverList.get(i),pathList.get(i));
					if(tempRet1==false||tempRet2==false)
					{
						System.out.println("Kill or Start server error!!!");
						return false;
					}
				}
			}
			
		}
		return bRet;
		
	}
	
    
// 得到其父目录
    public static String getParentDir(String filePath) 
    {
        if (null == filePath) 
        {
            log.error("file name null!");
            return null;
        }
        String strSlash = "/";
        if (filePath.equals(strSlash)) 
        {
            return filePath;
        }
        String tmpPath = filePath;
        if (filePath.endsWith(strSlash)) 
        {
            tmpPath = filePath.substring(0, filePath.length() - 1);
            log.debug("filePath: " + filePath + ", ends with '/', remove the last '/'!, name now: " + tmpPath);
        }
        int lastPos = filePath.lastIndexOf(strSlash);
        if (-1 == lastPos) 
        {
            log.error("invalid file name: " + filePath); 
            return null;
        }
        String parentDir = null;
        // parent is "/"
        if (0 == lastPos) 
        {
            parentDir = strSlash;
        }
        else 
        {
            parentDir = tmpPath.substring(0, lastPos);
        }
        log.debug("parent dir of: \"" + filePath + "\" is: " + parentDir);
        return parentDir;
    }
//
//得到本身的名字
    public static String getBaseName(String filePath) 
    {
        if (null == filePath) 
        {
            log.error("file name null!");
            return null;
        }
        String strSlash = "/";
        if (filePath.equals(strSlash)) 
        {
            return filePath;
        }
        String tmpPath = filePath;
        if (filePath.endsWith(strSlash)) 
        {
            tmpPath = filePath.substring(0, filePath.length() - 1);
            log.debug("filePath: " + filePath + ", ends with '/', remove the last '/'!, name now: " + tmpPath);
        }
        int lastPos = filePath.lastIndexOf(strSlash);
        if (-1 == lastPos) 
        {
            log.error("invalid file name: " + filePath); 
            return null;
        }
        String baseName = null;
        baseName = tmpPath.substring(lastPos + 1, tmpPath.length());
        log.debug("base name of: \"" + filePath + "\" is: " + baseName);
        return baseName;
    }

//
//分开本身的名字和父目录名字... 
    public static ArrayList<String> split(String filePath) 
    {
        if (null == filePath) 
        {
            log.error("file name null!");
            return null;
        }
        ArrayList<String> result = new ArrayList<String>();
        String strSlash = "/";
        // in case of "/", will return "/" and ""
        if (filePath.equals(strSlash)) 
        {
            result.add(strSlash);
            result.add(""); 
            return result;
        }
        String tmpPath = filePath;
        if (filePath.endsWith(strSlash)) 
        {
            tmpPath = filePath.substring(0, filePath.length() - 1);
            log.debug("filePath: " + filePath + ", ends with '/', remove the last '/'!, name now: " + tmpPath);
        }
        int lastPos = filePath.lastIndexOf(strSlash);
        if (-1 == lastPos) 
        {
            log.error("invalid file name: " + filePath); 
            return null;
        }
        String parentDir = null;
        // parent is "/"
        if (0 == lastPos)
        {
            parentDir = strSlash;
        }
        else 
        {
            parentDir = tmpPath.substring(0, lastPos);
        }
        String baseName = null;
        baseName = tmpPath.substring(lastPos + 1, tmpPath.length());
        log.debug("parent dir of: \"" + filePath + "\" is: " + parentDir);
        log.debug("base name of: \"" + filePath + "\" is: " + baseName);
        result.add(parentDir);
        result.add(baseName);
        return result;
    }
  
//
//拥有父子产生新目录   
    public static String join(String parentDir, String baseName) 
    {
        if (null == parentDir || null == baseName) 
        {
            log.error("parentDir or baseName null!");
            return null;
        }
        String strSlash = "/";
        if (parentDir.endsWith(strSlash)) 
        {
            return parentDir + baseName;
        }
        else 
        {
            return parentDir + "/" + baseName;
        }
    }
    
//    
//删除指定目录
    public static boolean rmDirRecursive(long appId, long userId, String dir)
    {
        boolean bRet = false;
        int iRet = -1;
        List<FileMetaInfo>  fileInfoList;
        FileMetaInfo  fileMetaInfo;
        // make sure this dir exist, check its parent
        boolean dirExist = false;
        ArrayList<String> dirComponent = split(dir);
        if (null == dirComponent || 2 != dirComponent.size())
        {
            log.error("split dir: \"" + dir + "\" failed!");
            return false;
        }
        String parentDir = dirComponent.get(0);
        String baseName = dirComponent.get(1);
        if (null == parentDir || null == baseName) 
        {
            log.error("parent dir or base name of dir: " + dir + "\" null!");
            return false;
        }
        fileInfoList = tfsManager.lsDir(appId, userId, parentDir);
        for (int i = 0; i < fileInfoList.size(); i++)
        {
            fileMetaInfo = fileInfoList.get(i);
            if (!fileMetaInfo.isFile() && fileMetaInfo.getFileName().equals(baseName)) 
            {
                dirExist = true;
                break;
            }
        }
        if (false == dirExist)
        {
            log.warn("dir: \"" + dir + "\" not exists!");
            return true;
        }
        
        // do the things
        Stack<NameMetaDirHelper> dirStack = new Stack<NameMetaDirHelper>();
        NameMetaDirHelper dirHelper = new NameMetaDirHelper(dir);
        dirStack.push(dirHelper);
        while (!dirStack.empty()) 
        {
            NameMetaDirHelper top = dirStack.peek();
            if (top.canBeDeleted())
            {
                tfsManager.rmDir(appId, userId, top.getDirName()); 
                dirStack.pop();
            }
            else 
            {
                fileInfoList = tfsManager.lsDir(appId, userId, top.getDirName());
                log.debug("fileInfoList size: " + fileInfoList.size());
                for (int i = 0; i < fileInfoList.size(); i++) 
                {
                    fileMetaInfo = fileInfoList.get(i);
                    if (fileMetaInfo.isFile()) 
                    {
                        iRet = tfsManager.rmFile(appId, userId, top.getDirName() + "/" + fileMetaInfo.getFileName());
                        if (iRet < 0) 
                        {
                            return false;
                        }
                    }
                    else 
                    {
                        dirHelper = new NameMetaDirHelper(top.getDirName() + "/" + fileMetaInfo.getFileName());
                        dirStack.push(dirHelper);                 
                    }
                }
                top.setCanBeDeleted(true);
            }
        }
        return true;
    }

 
}
