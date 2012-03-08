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
import org.junit.BeforeClass;
import org.junit.AfterClass;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.TfsManager;
import com.taobao.common.tfs.DefaultTfsManager;

public class tfsNameBaseCase
{
	public static DefaultTfsManager tfsManager=null ;
	public static String resourcesPath="";
    public static final Log log = LogFactory.getLog(tfsNameBaseCase.class);
    public static long appId;
    public static long userId=8;
    public static String key = "D:/staf/STAFReg.inf";
   
    

    static 
	{
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
        appId = tfsManager.getAppId();
    }
    
    public void deleteDir(String s,int n,TfsManager tfsManager)
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
