package com.taobao.common.tfs.testcase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement; 
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import com.taobao.common.tfs.utility.MySQLConnector;

import com.taobao.common.tfs.DefaultTfsManager;


public class rcTfsBaseCase extends BaseCase 
{
	//define list
	protected List<String> tfsNames = new ArrayList<String>();
	protected List<String> metaFiles = new ArrayList<String>();

	
	//rc server 配置文件的读取 (config&server)
	//...
	protected static Log log = LogFactory.getLog(rcTfsBaseCase.class);
	protected static MySQLConnector tfsSqlConnector = new MySQLConnector();
	protected static long appId = 0;
	
	
	//tair 相关配置项
	public static String localFile = "100K.jpg";
    public static String localFileL= "10M.jpg"; 
    public static String tairMasterAddr = "10.232.4.5:5198";
    public static String tairSlaveAddr = "10.232.4.5:5198";
    public static String tairGroupName = "group_1";

	//MySql 配置
	protected static String server = "10.232.36.208:3306";
	protected static String db = "tfs_stat_diqing";
	protected static String user = "foorbar";
	protected static String pwd = "foorbar";
	protected static Connection tfsSqlCon;
	

	
	// Define
	final public int RCINDEX = 0;//??
	final public int RCNSINDEX = 1;//??
	
	//混合操作预定比例
	public String caseName = "";
	final public int WAITTIME = 30;
	public int FETCH_OPER = 1;
	public int SAVE_OPER = 2;
	public int UNLINK_OPER = 4;

	//RcServer的一些配置量
	
	public static long userId = 8;
	public static String appKey = "testKeyYS";
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


	public static String RcIp="10.232.36.208";
	public static String RcPath="/home/admin/.../";
	
	//初始化rc 
	static {
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
		appId = tfsManager.getAppId();
		System.out.println("appid is : " + appId);
	}
	
	public DefaultTfsManager createTfsManager() {
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		DefaultTfsManager tfsManager = (DefaultTfsManager) appContext
				.getBean("tfsManager");
		appId = tfsManager.getAppId();

		return tfsManager;
	}
	

	/**
	 * MD5 哈希函数(对文件哈希)
	 * 
	 * @param path
	 * @return
	 */
	public static StringBuilder fileMd5(String path) {
		StringBuilder sb = new StringBuilder();
		StringBuilder noAlgorithm = new StringBuilder(
				"无MD5算法，这可能是你的JDK/JRE版本太低");
		StringBuilder fileNotFound = new StringBuilder("未找到文件，请重新定位文件路径");
		StringBuilder IOerror = new StringBuilder("文件流输入错误");
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5"); // 生成MD5类的实例
			File file = new File(path); // 创建文件实例，设置路径为方法参数
			FileInputStream fs = new FileInputStream(file);
			BufferedInputStream bi = new BufferedInputStream(fs);
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			byte[] b = new byte[bi.available()]; // 定义字节数组b大小为文件的不受阻塞的可访问字节数
			int i;
			// 将文件以字节方式读到数组b中
			while ((i = bi.read(b, 0, b.length)) != -1) {
			}
			md5.update(b);// 执行MD5算法
			for (byte by : md5.digest()) {
				sb.append(String.format("%02X", by)); // 将生成的字节MD５值转换成字符串
			}
			bo.close();
			bi.close();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			return noAlgorithm;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			return fileNotFound;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return IOerror;
		}
		return sb;// 返回MD5串
	}
	
	/**
     * MD5 哈希函数(对字符串哈希)
     * @param plainByte
     * @param num
     * @return
     */
    public static String md5(byte[] plainByte) { 
            try { 
                    MessageDigest md = MessageDigest.getInstance("MD5"); 
                    md.update(plainByte); 
                    byte b[] = md.digest(); 
                    int i; 
                    StringBuffer buf = new StringBuffer(""); 
                    for (int offset = 0; offset < b.length; offset++) { 
                            i = b[offset]; 
                            if(i<0) i+= 256; 
                            if(i<16) 
                            buf.append("0"); 
                            buf.append(Integer.toHexString(i)); 
                    } 
                    return buf.toString();
            } catch (Exception e) { 
                    e.printStackTrace(); 
            } 
            return null;
    } 

	 protected byte[]  getByte(String fileName) throws IOException
	    {
	    	   InputStream in = new FileInputStream(fileName);
	           byte[] data= new byte[in.available()];
	           in.read(data);
	           return data;
	    }
	
	/*server 控制函数*/
	public boolean killOneRc(String Ip,String Path) 
	{
		boolean bRet = false;
		bRet=killOneServer(Ip,Path);
		return bRet;
	}
	public boolean startOneRc(String Ip,String Path) 
	{
		boolean bRet = false;
		bRet=startOneServer(Ip,Path);
		return bRet;
	}
	
	
	
	/*Rc 查看于Rc所提供服务的stat的相关信息*/
	public long getUsedCapacity(String app_id) 
	{
		long iRet = -1;
		boolean bRet = false;
		
		ResultSet result; 
		String SqlState = "select used_capacity  from t_app_stat where app_id = "+app_id+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			if(bRet)
			{
				result = sqlStateMent.getResultSet();
				iRet = result.getLong(0);
			}
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		return iRet;
	}

	public long getMaxQuote(String app_id) 
	{
		long iRet = -1;
		boolean bRet = false;
		
		ResultSet result; 
		String SqlState = "select quto from t_app_info where id = "+app_id+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			if(bRet)
			{
				result = sqlStateMent.getResultSet();
				iRet = result.getLong(0);
			}
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		return iRet;
	}
	public long getCurrentQuote(String app_id)
	{
		long iRet = -1;
		iRet=getMaxQuote(app_id)-getUsedCapacity(app_id);
		if (iRet<0)
			iRet = -1;
		return iRet;
	}
	
	public long getFileCount(String app_id) 
	{
		long iRet = -1;
		boolean bRet = false;
		
		ResultSet result; 
		String SqlState = "select file_count from t_app_info where id = "+app_id+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			if(bRet)
			{
				result = sqlStateMent.getResultSet();
				iRet = result.getLong(0);
			}
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		return iRet;
	}

	public long getOperTimes(String sessionId,int oper_type) 
	{
		long iRet = -1;
		boolean bRet = false;
		
		ResultSet result; 
		String SqlState = "select oper_times from t_session_stat where session_id = '"+sessionId+"' and oper_type = "+oper_type+" ;";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			if(bRet)
			{
				result = sqlStateMent.getResultSet();
				iRet = result.getLong(0);
			}
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		return iRet;
	}

	public long getSuccTimes(String sessionId,int oper_type) 
	{
		long iRet = -1;
		boolean bRet = false;
		
		ResultSet result; 
		String SqlState = "select succ_times from t_session_stat where session_id = '"+sessionId+"' and oper_type = "+oper_type+" ;";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			if(bRet)
			{
				result = sqlStateMent.getResultSet();
				iRet = result.getLong(0);
			}
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		return iRet;
	}

	public long getFileSize(String sessionId,int oper_type) 
	{
		long iRet = -1;
		boolean bRet = false;
		
		ResultSet result; 
		String SqlState = "select file_size from t_session_stat where session_id = '"+sessionId+"' and oper_type = "+oper_type+" ;";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			if(bRet)
			{
				result = sqlStateMent.getResultSet();
				iRet = result.getLong(0);
			}
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		return iRet;
	}
	
//	public int getClusterEx(String nsAddr);
//	public int getClusterIndex(String tfsname);
	
	//rc容量相关变更函数
	public boolean modifyMaxQuote(String app_id , long newQuote) 
	{
	
		boolean bRet = false;

		String SqlState = "update t_app_info set quto="+newQuote+" where id ="+app_id+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			sqlStateMent.execute("update t_base_info_update_time set base_last_update_time=now();");
			sqlStateMent.execute("update t_base_info_update_time set app_last_update_time=now();");
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return bRet;
	}
	
	//Rc权限控制设置

	public boolean setGroupPermission(String cluster_group_id, int cluster_rack_access_type) 
	{
	
		boolean bRet = false;
		 
		String SqlState = "update t_cluster_rack_group set cluster_rack_access_type="+cluster_rack_access_type+" where cluster_group_id ="+cluster_group_id+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			sqlStateMent.execute("update t_base_info_update_time set base_last_update_time=now();");
			sqlStateMent.execute("update t_base_info_update_time set app_last_update_time=now();");
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return bRet;
	}

	public boolean setClusterPermission(String cluster_rack_id, int cluster_stat) 
	{
	
		boolean bRet = false;
		 
		String SqlState = "update t_cluster_rack_group set cluster_stat="+cluster_stat+" where cluster_rack_id ="+cluster_rack_id+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			sqlStateMent.execute("update t_base_info_update_time set base_last_update_time=now();");
			sqlStateMent.execute("update t_base_info_update_time set app_last_update_time=now();");
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return bRet;
	}
	
//	public boolean checkSessionId(String sessionId);//测试不需要该函数
	
	//Rc排重相应设置
	public boolean setDuplicateStatus(String appId, int status)
	{
	
		boolean bRet = false;
		 
		String SqlState = "update t_app_info set need_duplicate="+status+" where id ="+appId+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			sqlStateMent.execute("update t_base_info_update_time set base_last_update_time=now();");
			sqlStateMent.execute("update t_base_info_update_time set app_last_update_time=now();");
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return bRet;
	}
	
		public boolean changeDuplicateServerAddr(String cluster_rack_id, String dupliate_server_addr)
	{
	
		boolean bRet = false;
		 
		String SqlState = "update t_cluster_rack_duplicate_server set dupliate_server_addr="+dupliate_server_addr+" where cluster_rack_id ="+cluster_rack_id+";";
		try 
		{
			Statement sqlStateMent = tfsSqlCon.createStatement();
			bRet = sqlStateMent.execute(SqlState);
			sqlStateMent.execute("update t_base_info_update_time set base_last_update_time=now();");
			sqlStateMent.execute("update t_base_info_update_time set app_last_update_time=now();");
			
		} 
		catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return bRet;
	}

}