package com.taobao.common.tfs.testcase.Interface.normal.rc;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;



public class RcTfsManager_07_openWriteFile extends rcTfsBaseCase 
{
	
	@Test
    public  void  test_01_saveFile_then_openWriteFile_wrong_tfsFileName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String name="hsadkshadfksafkjsa";
		
		int fd=-1;
		fd=tfsManager.openWriteFile(name, null, key);
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_02_saveFile_then_openWriteFile_null_tfsFileName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String name=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(name, null, key);
		System.out.println(fd);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_03_saveFile_then_openWriteFile_empty_tfsFileName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String name="";
		
		int fd=-1;
		fd=tfsManager.openWriteFile(name, null, key);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_04_saveLargeFile_then_openWriteFile()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		System.out.println(fd);
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_05_saveFile_then_openWriteFile()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		System.out.println(fd);
		Assert.assertTrue(fd<0);
	}
	
    public  void  test_05_saveLargeFile_then_openWriteFile_wrong_suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, ".jpg", key);
		Assert.assertTrue(fd<0);
	}
	
	
    public  void  test_06_saveLargeFile_Suffix_then_openWriteFile_Suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"10M.jpg",null,".jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, ".jpg", key);
		Assert.assertTrue(fd>0);
	}
	
	
    public  void  test_07_saveLargeFile_byte_then_openWriteFile() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"10M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
	}
	
	
    public  void  test_08_saveLargeFile_byte_then_openWriteFile_wrong_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"10M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, ".jpg", key);
		Assert.assertTrue(fd<0);
	}
	
	
    public  void  test_09_saveLargeFile_byte_Suffix_then_openWriteFile_Suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"10M.jpg");
		Ret=tfsManager.saveLargeFile(null,".jpg",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, ".jpg", key);
		Assert.assertTrue(fd<0);
	}
}