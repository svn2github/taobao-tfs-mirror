package com.taobao.common.tfs.rcitest;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.tfsNameBaseCase;

public class RcTfsManager_14_hideFile extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_saveFile_then_hideFile()
	{
		log.info( "test_01_saveFile_then_hideFile" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		Bret=tfsManager.hideFile(Ret, null, 0);
		Assert.assertTrue(Bret);
	}
	
	@Test
    public  void  test_02_saveFile_simpleName_then_hideFile()
	{
		log.info( "test_02_saveFile_simpleName_then_hideFile" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		Bret=tfsManager.hideFile(Ret, null, 0);
		Assert.assertTrue(Bret);
	}
	
	@Test
    public  void  test_03_saveFile_byte_then_hideFile() throws IOException
	{
		log.info( "test_03_saveFile_byte_then_hideFile" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		Bret=tfsManager.hideFile(Ret, null, 0);
		Assert.assertTrue(Bret);
	}
	
	@Test
    public  void  test_04_saveFile_byte_simpleName_then_hideFile() throws IOException
	{
		log.info( "test_04_saveFile_byte_simpleName_then_hideFile" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		Bret=tfsManager.hideFile(Ret, null, 0);
		Assert.assertTrue(Bret);
	}
	
	@Test
    public  void  test_05_saveLargeFile_then_hideFile()
	{
		log.info( "test_05_saveLargeFile_then_hideFile" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		Bret=tfsManager.hideFile(Ret, null, 0);
		Assert.assertTrue(Bret);
	}
	
	@Test
    public  void  test_06_saveLargeFile_byte_then_hideFile() throws IOException
	{
		log.info( "test_06_saveLargeFile_byte_then_hideFile" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		Bret=tfsManager.hideFile(Ret, null, 0);
		Assert.assertTrue(Bret);
	}
	
	@Test
    public  void  test_07_hideFile_wrong_tfsFileName()
	{
		log.info( "test_07_hideFile_wrong_tfsFileName" );
		
		boolean Bret;
		Bret=tfsManager.hideFile("hsakjdhskasjka", null, 1);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_08_hideFile_null_tfsFileName()
	{
		log.info( "test_08_hideFile_null_tfsFileName" );
		
		boolean Bret;
		Bret=tfsManager.hideFile(null, null, 1);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_09_hideFile_empty_tfsFileName()
	{
		log.info( "test_09_hideFile_empty_tfsFileName" );
		
		boolean Bret;
		Bret=tfsManager.hideFile("", null, 1);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_10_openReadFile_then_hideFile() throws IOException
	{
		log.info( "test_10_openReadFile_then_hideFile" );
	
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		
		int Rret;
		byte []data=null;
		data = new byte [100*(1<<10)];
		Rret=tfsManager.readFile(fd, data,0, 100*(1<<10));
		Assert.assertTrue(Rret<0);
	}
	
	
    public  void  test_11_openWriteFile_then_hideFile() throws IOException
	{
		log.info( "test_11_openWriteFile_then_hideFile" );
	
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null,key);
		Assert.assertTrue(fd>0);
		Ret=tfsManager.closeFile(fd);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5m.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertTrue(Wret<0);
	}
	
	@Test
    public  void  test_12_saveFile_then_hideFile_wrong_suffix()
	{
		log.info( "test_12_saveFile_then_hideFile_wrong_suffix" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, ".jpg", 1);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_13_saveFile_then_hideFile_empty_suffix()
	{
		log.info( "test_13_saveFile_then_hideFile_empty_suffix" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, "", 1);
		Assert.assertTrue(Bret);
	}
	
	@Test
    public  void  test_14_saveFile_then_hideFile_double()
	{
		log.info( "test_14_saveFile_then_hideFile_double" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertTrue(Bret);
		Bret=tfsManager.hideFile(Ret, null, 1);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_15_saveFile_then_unhideFile()
	{
		log.info( "test_15_saveFile_then_unhideFile" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, null, 0);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_16_saveFile_with_suffix_then_hideFile_with_suffix()
	{
		log.info( "test_16_saveFile_with_suffix_then_hideFile_with_suffix" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.hideFile(Ret, ".jpg", 1);
		Assert.assertTrue(Bret);
	}
}