package com.taobao.common.tfs.testcase.Interface.normal.rc;


import java.io.IOException;


import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;


public class RcTfsManager_12_statFile extends rcTfsBaseCase 
{
	@Test
    public  void  test_01_statFile_null_tfsFileName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(null, null);
		Assert.assertNull(info);
	}
	
	@Test
    public  void  test_02_statFile_empty_tfsFileName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		FileInfo info=new FileInfo();
		info=tfsManager.statFile("", null);
		Assert.assertNull(info);
	}
	
	@Test
    public  void  test_03_statFile_wrong_tfsFileName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		FileInfo info=new FileInfo();
		info=tfsManager.statFile("skhskjahfskahfka", null);
		Assert.assertNull(info);
	}
	
	@Test
    public  void  test_04_saveFile_statFile()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, null);
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_05_saveFile_simpleName_statFile()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, null);
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_06_saveFile_statFile_wrong_suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNull(info);	
	}
	
	@Test
    public  void  test_07_saveFile_with_suffix_statFile_with_suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_08_saveFile_simpleName_with_suffix_statFile_with_suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_09_saveFile_byte_statFile() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, null);
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_10_saveFile_byte_simpleName_statFile() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, null);
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_11_saveFile_byte_statFile_wrong_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNull(info);
	}
	
	@Test
    public  void  test_12_saveFile_byte_with_suffix_statFile_with_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_13_saveFile_byte_simpleName_with_suffix_statFile_with_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 100*(1<<10));	
	}
	
	@Test
    public  void  test_14_saveLargeFile_statFile() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, null);
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 5*(1<<20));	
	}
	
	@Test
    public  void  test_15_saveLargeFile_statFile_wrong_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNull(info);	
	}
	
	@Test
    public  void  test_16_saveLargeFile_suffix_statFile_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,".jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 5*(1<<20));	
	}
	
	@Test
    public  void  test_17_saveLargeFile_byte_statFile() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, null);
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 5*(1<<20));	
	}
	
	@Test
    public  void  test_18_saveLargeFile_byte_statFile_wrong_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNull(info);	
	}
	
	@Test
    public  void  test_19_saveLargeFile_byte_with_suffix_statFile_with_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,".jpg",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, ".jpg");
		Assert.assertNotNull(info);
		Assert.assertEquals(info.getLength(), 5*(1<<20));	
	}
	
	@Test
    public  void  test_20_saveFile_unlinkFile_statFile()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		tfsManager.unlinkFile(Ret, null);
		
		FileInfo info=new FileInfo();
		info=tfsManager.statFile(Ret, null);
		//Assert.assertNull(info);
		System.out.println(info.toString());
	}
	
}