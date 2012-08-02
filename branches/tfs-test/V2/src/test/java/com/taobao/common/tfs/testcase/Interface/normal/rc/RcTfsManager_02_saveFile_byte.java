package com.taobao.common.tfs.testcase.Interface.normal.rc;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;



public class RcTfsManager_02_saveFile_byte extends rcTfsBaseCase
{
	@Test
    public  void  test_01_saveFile_byte() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_02_saveFile_byte_less_length() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,10*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_03_saveFile_byte_more_length() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,200*(1<<10),false);
		Assert.assertNotNull(Ret);
	}
	
	@Test
    public  void  test_04_saveFile_byte_wrong_offset() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,-1,100*(1<<10),false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_05_saveFile_byte_with_offset_and_length() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,10*(1<<10),60*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_06_saveFile_byte_with_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_07_saveFile_byte_with_empty_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,"",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_08_saveFile_byte_with_empty_data() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		String buf="";
		data=buf.getBytes();
		Ret=tfsManager.saveFile(null,null,data,0,0,false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_09_saveFile_byte_with_null_data() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		Ret=tfsManager.saveFile(null,null,data,0,0,false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_10_saveFile_byte_with_wrong_length() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,-1,false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_11_saveFile_byte_with_tfsname() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.unlinkFile(Ret, null);
		Assert.assertTrue(Bret);
		
		String name= Ret;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(name,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_12_saveFile_byte_with_tfsname_and_suffix() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.unlinkFile(Ret, null);
		Assert.assertTrue(Bret);
		
		String name= Ret;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(name,".txt",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_13_saveFile_byte_with_wrong_tfsname() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile("Tis521423695781236",null,data,0,100*(1<<10),false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_14_saveFile_byte_Large() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"10m.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,10*(1<<20),false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_15_saveFile_byte_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_16_saveFile_byte_less_length_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,10*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_17_saveFile_byte_more_length_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,200*(1<<10),true);
		Assert.assertNotNull(Ret);
	}
	
	@Test
    public  void  test_18_saveFile_byte_wrong_offset_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,-1,100*(1<<10),true);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_19_saveFile_byte_with_offset_and_length_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,10*(1<<10),60*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_20_saveFile_byte_with_suffix_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_21_saveFile_byte_with_empty_suffix_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,"",data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_22_saveFile_byte_with_empty_data_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		String buf="";
		data=buf.getBytes();
		Ret=tfsManager.saveFile(null,null,data,0,0,true);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_23_saveFile_byte_with_null_data_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		Ret=tfsManager.saveFile(null,null,data,0,0,true);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_24_saveFile_byte_with_wrong_length_simpleName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,-1,true);
		Assert.assertNull(Ret);
	}
	
	
}