package com.taobao.common.tfs.testcase.Interface.normal.rc;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;



public class RcTfsManager_09_readFile extends rcTfsBaseCase 
{
	@Test
    public  void  test_01_openReadFile_then_readFile() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		int savecrc;
		int readcrc;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte data[]=null;
		data=new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, data,0, 5*(1<<20));
		Assert.assertEquals(Rret,5*(1<<20));
		
		
		FileUtility.dataToFile(resourcesPath+"readtemp",data);
		readcrc=FileUtility.getCrc(resourcesPath+"readtemp");
		Assert.assertEquals(savecrc,readcrc);		
	}
	
	@Test
    public  void  test_02_openReadFile_then_readFile_less_length() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data=new byte [4*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, data,0, 4*(1<<20));
		Assert.assertEquals(Rret,4*(1<<20));
			
	}
	
	@Test
    public  void  test_03_openReadFile_then_readFile_more_length() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data=new byte [5*(1<<20)+1025];
		int Rret;
		Rret=tfsManager.readFile(fd, data,1, 5*(1<<20)+1024);
		System.out.println(Rret);
		Assert.assertEquals(Rret,5*(1<<20));			
	}
	
	@Test
    public  void  test_04_openReadFile_then_readFile_with_offset() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data=new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, data,1<<20, 4*(1<<20));
		System.out.println(Rret);
		Assert.assertEquals(Rret,4*(1<<20));
			
	}
	
	@Test
    public  void  test_05_openReadFile_then_readFile_more_offset() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data=new byte [5*(1<<20)+2];
		int Rret;
		Rret=tfsManager.readFile(fd, data,5*(1<<20)+1, 1);
		Assert.assertEquals(Rret,1);		
	}
	
	@Test
    public  void  test_06_openReadFile_then_readFile_wrong_offset() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data=new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, data,-1, 5*(1<<20));
		Assert.assertTrue(Rret<0);			
	}
	
	@Test
    public  void  test_07_openReadFile_then_readFile_more_offset_and_length() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data=new byte [5*(1<<20)+10];
		int Rret;
		Rret=tfsManager.readFile(fd,data,10,5*(1<<20));
		Assert.assertEquals(Rret,5*(1<<20));			
	}
	
	@Test
    public  void  test_08_openReadFile_then_readFile_with_wrong_fd() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
	
		int fd=-1;
		byte[] data=null;
		data=new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, data,0, 5*(1<<20));
		Assert.assertTrue(Rret<0);
		
	}
	
	@Test
    public  void  test_09_openReadFile_then_readFile_with_wrong_fd_2() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
	
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		tfsManager.closeFile(fd);
		
		int Rret;
		byte []data=null;
		data=new byte [5*(1<<20)];
		Rret=tfsManager.readFile(fd, data,0, 5*(1<<20));
		Assert.assertTrue(Rret<0);
	}
	
	@Test
    public  void  test_10_openReadFile_then_readFile_with_wrong_fd_3() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
	
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		tfsManager.unlinkFile(Ret, null);
		
		int Rret;
		byte []data=null;
		data=new byte [5*(1<<20)];
		
		Rret=tfsManager.readFile(fd, data,0, 5*(1<<20));
		Assert.assertTrue(Rret<0);
		
	}
	
	
	@Test
    public  void  test_11_openReadFile_then_readFile_null_data() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		int Rret;
		Rret=tfsManager.readFile(fd,null,0,0);
		Assert.assertTrue(Rret<0);			
	}
	
	
	
}