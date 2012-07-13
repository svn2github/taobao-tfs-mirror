package com.taobao.common.tfs.testcase.Interface.normal.rc;


import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;



public class RcTfsManager_08_openReadFile extends rcTfsBaseCase 
{
	@Test
    public  void  test_01_saveFile_then_openReadFile()
	{
		log.info( "test_01_saveFile_then_openReadFile" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_02_saveFile_simpleName_then_openReadFile()
	{
		log.info( "test_02_saveFile_simpleName_then_openReadFile" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_03_saveFile_then_openReadFile_wrong_suffix()
	{
		log.info( "test_03_saveFile_then_openReadFile_wrong_suffix" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_04_saveFile_suffix_then_openReadFile_suffix()
	{
		log.info( "test_04_saveFile_suffix_then_openReadFile_suffix");
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_05_saveFile_suffix_simpleName_then_openReadFile_suffix()
	{
		log.info( "test_05_saveFile_suffix_simpleName_then_openReadFile_suffix");
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_06_saveFile_then_openReadFile_wrong_tfsFileName()
	{
		log.info( "test_06_saveFile_then_openReadFile_wrong_tfsFileName" );
		String name="hsadkshadfksafkjsa";
		
		int fd=-1;
		fd=tfsManager.openReadFile(name, null);
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_07_saveFile_then_openReadFile_null_tfsFileName()
	{
		log.info( "test_07_saveFile_then_openReadFile_null_tfsFileName" );
		String name=null;
		
		int fd=-1;
		fd=tfsManager.openReadFile(name, null);
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_08_saveFile_then_openReadFile_empty_tfsFileName()
	{
		log.info( "test_08_saveFile_then_openReadFile_empty_tfsFileName" );
		String name="";
		
		int fd=-1;
		fd=tfsManager.openReadFile(name, null);
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_09_saveFile_byte_then_openReadFile() throws IOException
	{
		log.info( "test_09_saveFile_byte_then_openReadFile" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_10_saveFile_byte_simpleName_then_openReadFile() throws IOException
	{
		log.info( "test_10_saveFile_byte_simpleName_then_openReadFile" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_11_saveFile_byte_then_openReadFile_wrong_suffix() throws IOException
	{
		log.info( "test_11_saveFile_byte_then_openReadFile_wrong_suffix" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_12_saveFile_byte_suffix_then_openReadFile_suffix() throws IOException
	{
		log.info( "test_12_saveFile_byte_suffix_then_openReadFile_suffix" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_13_saveFile_byte_simpleName_suffix_then_openReadFile_suffix() throws IOException
	{
		log.info( "test_13_saveFile_byte_simpleName_suffix_then_openReadFile_suffix" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_14_saveLargeFile_then_openReadFile()
	{
		log.info( "test_14_saveLargeFile_then_openReadFile" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_15_saveLargeFile_then_openReadFile_wrong_suffix()
	{
		log.info( "test_15_saveLargeFile_then_openReadFile_wrong_suffix" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_16_saveLargeFile_Suffix_then_openReadFile_Suffix()
	{
		log.info( "test_16_saveLargeFile_Suffix_then_openReadFile_Suffix" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,".jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_17_saveLargeFile_byte_then_openReadFile() throws IOException
	{
		log.info( "test_17_saveLargeFile_byte_then_openReadFile" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
	}
	
	@Test
    public  void  test_18_saveLargeFile_byte_then_openReadFile_wrong_suffix() throws IOException
	{
		log.info( "test_18_saveLargeFile_byte_then_openReadFile_wrong_suffix" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd<0);
	}
	
	@Test
    public  void  test_19_saveLargeFile_byte_Suffix_then_openReadFile_Suffix() throws IOException
	{
		log.info( "test_19_saveLargeFile_byte_Suffix_then_openReadFile_Suffix" );
		String Ret=null;
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,".jpg",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, ".jpg");
		Assert.assertTrue(fd>0);
	}
}