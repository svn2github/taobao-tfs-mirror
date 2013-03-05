package com.taobao.common.tfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import java.util.Random;
import java.util.zip.CRC32;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.etao.gaia.exception.OperationException;
import com.etao.gaia.handler.OperationResult;
import com.etao.gaia.handler.ProcessHandler;
import com.etao.gaia.handler.staf.ProcessHandlerSTAFImpl;




public class RestfulTfsBaseCase
{
	public static DefaultTfsManager tfsManager=null ;
	public static String resourcesPath="";
    public static final Log log = LogFactory.getLog(RestfulTfsBaseCase.class);
    public static long appId;
    public static long userId=8;
    public static String key = "D:/tfs.rar";
    
    public static String UserName = "admin";
    public static String MachineIp = "10.232.36.210";
    
    
    public static String tfstool = "/home/yiming.czw/tfs_bin/bin/tfstool";
    public static String Kv_Meta_Root = " -k 10.232.36.210:7201";
    public static String Name_Server = " -s 10.232.36.202:5202";
    public static String TempFile = "/tmp/Temp";
    public static ProcessHandler PH = new ProcessHandlerSTAFImpl();
    
    public static String buecket_name = "";

    static 
	{
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
        appId = tfsManager.getAppId();
        buecket_name = Long.toString(appId)+"/"+Long.toString(userId);
    }
    
	public  OperationResult Executer(String UserName, String MachineIp, String Cmd, boolean IsBackgroud)
	{
		OperationResult Result = new OperationResult();
		try 
		{
			System.out.println("Cmd is :" + Cmd);
			Result = PH.executeCmdByUser(UserName, MachineIp, Cmd, IsBackgroud);
			System.out.println("Result is :" + Result.getMsg());
		} 
		catch (OperationException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.assertTrue(false);
		}
		return Result;
	}
	
	public boolean HeadObject(String bucket_name, String object_name, int ExpectLen)
	{
		boolean bRet = false ;
		OperationResult Result = new OperationResult();
		String Cmd = tfstool + Kv_Meta_Root + Name_Server + 
		" -i 'head_object "+bucket_name +" "+object_name +"' 2>/dev/null";
		Result  = Executer(UserName,MachineIp,Cmd,false);
		if(Result.getMsg().contains("success"))
		{
			Cmd = tfstool + Kv_Meta_Root + Name_Server + 
			" -i 'head_object "+bucket_name +" "+object_name +"' 2>/dev/null |awk 'NR==1{print \\$6}'";
			Result  = Executer(UserName,MachineIp,Cmd,false);
			StringBuffer sbuf = new  StringBuffer(Result.getMsg());
			sbuf.deleteCharAt(sbuf.length()-1);
			System.out.println(Integer.parseInt(sbuf.toString()));
			Assert.assertEquals(ExpectLen,Integer.parseInt(sbuf.toString()));
			return true;
		}
		return bRet;
	}
	
	public boolean HeadObject(String bucket_name, String object_name)
	{
		OperationResult Result = new OperationResult();
		String Cmd = tfstool + Kv_Meta_Root + Name_Server + 
		" -i 'head_object "+bucket_name +" "+object_name +"' 2>/dev/null";
		Result  = Executer(UserName,MachineIp,Cmd,false);
		return Result.getMsg().contains("success");
	}
	
	public boolean GetObject(String bucket_name, String object_name, String local_file)
	{
		OperationResult Result = new OperationResult();
		String Cmd = tfstool + Kv_Meta_Root + Name_Server + 
		" -i 'get_object "+bucket_name +" "+object_name +" "+local_file+"' 2>/dev/null";
		Result  = Executer(UserName,MachineIp,Cmd,false);
		return Result.getMsg().contains("success");
	}
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
    
    protected int getCrc(OutputStream opstream) 
	{
        try 
		{
        	String str = opstream.toString();        
            byte [] data = str.getBytes();
            CRC32 crc = new CRC32();
            crc.reset();
            crc.update(data);
            System.out.println(crc.getValue());
            return (int)crc.getValue();
        } 
		catch (Exception e) 
		{
            e.printStackTrace();
        }
        return 0;
    }
    protected int getCrc(String fileName) 
	{
        try 
		{
            FileInputStream input = new FileInputStream(fileName);        
            int readLength;
            byte[] data = new byte[102400];
            CRC32 crc = new CRC32();
            crc.reset();
            while ((readLength = input.read(data, 0, 102400)) > 0) 
			{
                crc.update(data, 0, readLength);
            }
            input.close();
            System.out.println(" file name crc " + fileName + " c " + crc.getValue());
            
            return (int)crc.getValue();
        } 
		catch (Exception e) 
		{
            e.printStackTrace();
        }
        return 0;
    }  
    protected byte[]  getByte(String fileName) throws IOException
    {
    	   InputStream in = new FileInputStream(fileName);
           byte[] data= new byte[in.available()];
           in.read(data);
           return data;
    }
    
	public static ArrayList<String> testFileList = new ArrayList<String>();

	@BeforeClass
	public static void BeforeSetup() 
	{
		System.out.println(" @@ beforeclass begin");
		String localFile = "";
		// 1b file
		localFile = "1B.jpg";
		if (createFile(localFile, 1)) 
		{
			testFileList.add(localFile);
		}
		localFile = "2B.jpg";
		if (createFile(localFile, 2)) 
		{
			testFileList.add(localFile);
		}
		// 1k file
		localFile = "1K.jpg";
		if (createFile(localFile, 1 << 10)) 
		{
			testFileList.add(localFile);
		}
		// 10k file
		localFile = "10K.jpg";
		if (createFile(localFile, 10 * (1 << 10)) ) 
		{
			testFileList.add(localFile);
		}
		// 10k file
		localFile = "1M.jpg";
		if (createFile(localFile, 1 << 20) ) 
		{
			testFileList.add(localFile);
		}
		// 100k file
		localFile = "100K.jpg";
		if (createFile(localFile, 100 * (1 << 10)) ) 
		{
			testFileList.add(localFile);
		}
		// 2M file
		localFile = "2M.jpg";
		if (createFile(localFile, 2 * (1 << 20))) 
		{
			testFileList.add(localFile);
		}
		// 3M file
		localFile = "3M.jpg";
		if (createFile(localFile, 3 * (1 << 20))) 
		{
			testFileList.add(localFile);
		}
		// 5M file
		localFile = "5M.jpg";
		if (createFile(localFile, 5 * (1 << 20))) 
		{
			testFileList.add(localFile);
		}
		// 10M file
		localFile = "10M.jpg";
		if (createFile(localFile, 10 * (1 << 20))) 
		{
			testFileList.add(localFile);
		}
		// 100M file
		localFile = "100M.jpg";
		if (createFile(localFile, 100 * (1 << 20))) 
		{
			testFileList.add(localFile);
		}
		// 1G file
		localFile = "1G.jpg";
		if (createFile(localFile, 1 << 30)) 
		{
			testFileList.add(localFile);
		}
		
	}

	@AfterClass
	public static void AfterTearDown() 
	{

		System.out.println(" @@ afterclass begin");
		int size = testFileList.size();
		for (int i = 0; i < size; i++) 
		{
			File file = new File(testFileList.get(i));
			if (file.exists()) 
			{
				file.delete();
			}
		}
	}

	protected static boolean createFile(String filePath, long size) 
	{
		boolean ret = true;
		try 
		{
			RandomAccessFile f = new RandomAccessFile(filePath, "rw");
			f.setLength(size);
		} 
		catch (Exception e) 
		{
			ret = false;
			e.printStackTrace();
		}
		return ret;
	}
	
	protected static void DataToFile(String filename,byte data[]) throws IOException
	{
		File file=new File(filename);  
        OutputStream out=new FileOutputStream(file);  
        out.write(data);  
        out.close();  
	}

}
