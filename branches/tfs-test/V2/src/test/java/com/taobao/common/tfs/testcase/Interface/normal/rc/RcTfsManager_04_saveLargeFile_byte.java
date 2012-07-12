package com.taobao.common.tfs.RcITest_2_2_3;

import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;

public class RcTfsManager_04_saveLargeFile_byte extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_saveLargeFile_byte() throws IOException
	{
    	log.info( "test_01_saveLargeFile_byte" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_02_saveLargeFile_byte_less_length() throws IOException
	{
    	log.info( "test_02_saveLargeFile_byte_less_length" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length-10*(1<<10),key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_03_saveLargeFile_byte_more_length() throws IOException
	{
    	log.info( "test_03_saveLargeFile_byte_more_length" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length+10*(1<<20),key);
		Assert.assertNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_04_saveLargeFile_byte_wrong_offset() throws IOException
	{
    	log.info( "test_04_saveLargeFile_byte_wrong_offset" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,-1,data.length,key);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_05_saveLargeFile_byte_more_offset() throws IOException
	{
    	log.info( "test_05_saveLargeFile_byte_more_offset" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,data.length+1,1,key);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_06_saveLargeFile_byte_more_offset_and_length() throws IOException
	{
    	log.info( "test_06_saveLargeFile_byte_more_offset_and_length" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,1<<20,data.length-2*(1<<20),key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_07_saveLargeFile_byte_with_suffix() throws IOException
	{
    	log.info( "test_07_saveLargeFile_byte_with_suffix" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,".txt",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_08_saveLargeFile_byte_with_empty_suffix() throws IOException
	{
    	log.info( "test_08_saveLargeFile_byte_with_empty_suffix" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,"",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_09_saveLargeFile_byte_with_empty_data() throws IOException
	{
    	log.info( "test_09_saveLargeFile_byte_with_empty_data" );
		String Ret=null;
		byte data[]=null;
		String buf="";
		data=buf.getBytes();
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_10_saveLargeFile_byte_with_null_data() throws IOException
	{
    	log.info( "test_10_saveLargeFile_byte_with_null_data" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile(null,null,null,0,0,key);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_11_saveLargeFile_byte_with_wrong_length() throws IOException
	{
    	log.info( "test_11_saveLargeFile_byte_with_wrong_length" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,-1,key);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_12_saveLargeFile_byte_small() throws IOException
	{
    	log.info( "test_12_saveLargeFile_byte_small" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"10k.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_13_saveLargeFile_byte_wrong_tfsname() throws IOException
	{
    	log.info( "test_13_saveLargeFile_byte_wrong_tfsname" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile("soahdksadiusaikjj",null,data,0,data.length,key);
		Assert.assertNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_14_saveLargeFile_byte_empty_tfsname() throws IOException
	{
    	log.info( "test_14_saveLargeFile_byte_empty_tfsname" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile("",null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_15_saveLargeFile_byte_null_key() throws IOException
	{
    	log.info( "test_15_saveLargeFile_byte_null_key" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,null);
		Assert.assertNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
}