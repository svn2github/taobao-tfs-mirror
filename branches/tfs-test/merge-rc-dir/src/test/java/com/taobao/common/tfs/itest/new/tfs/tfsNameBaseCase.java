package com.taobao.common.tfs.test.new.tfs;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.TfsManager;
import com.taobao.common.tfs.DefaultTfsManager;

public class tfsNameBaseCase
{
	public static TfsManager tfsManager=null ;
	public static DefaultTfsManager DftfsManager=null ;
	public static String resourcesPath="D:/workspace/merge-rc-dir/src/test/resources/";
    public static final Log log = LogFactory.getLog(tfsNameBaseCase.class);
    public static long appId;
    public static long userId=8;
   
    

    static 
	{
    	
    	System.out.println("!!!!!!!!!!!!!!!!!!get bean begin");
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        System.out.println("@@@@@@@@@@@get bean end");
        tfsManager = (TfsManager) appContext.getBean("tfsManager");
        DftfsManager = (DefaultTfsManager)tfsManager;
        appId = DftfsManager.getAppId();
        System.out.println("#####################"+appId);
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
    
    static protected void createFile(String name,long num ) throws FileNotFoundException, IOException
    {
       Random rd = new Random();
       File file = new File(name);
       FileOutputStream output = new FileOutputStream(file);
       int len = 1024*1024*8;
       byte[] data = new byte[len];
       long totalLength=num;
       while (totalLength > 0)
       {
          len = (int)Math.min(len, totalLength);
          rd.nextBytes(data);
          output.write(data, 0, len);
          totalLength -= len;
       }
       System.out.println("Succeed create file"+name);
    }
//     static // create resources files
//    {
//    	try {
//			createFile(resourcesPath+"100K",100*(1<<10));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//    	try {
//			createFile(resourcesPath+"1G",1<<30);
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//    	try {
//			createFile(resourcesPath+"2M",2*(1<<20));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//	 	try {
//			createFile(resourcesPath+"3M",3*(1<<20));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//	  	try {
//			createFile(resourcesPath+"10K",10*(1<<10));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    
//  	try {
//			createFile(resourcesPath+"2b",2);
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    
//  	try {
//			createFile(resourcesPath+"10M",10*(1<<20));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//  }
}
