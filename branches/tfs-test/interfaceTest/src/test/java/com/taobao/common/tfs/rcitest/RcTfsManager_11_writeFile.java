package com.taobao.common.tfs.rcitest;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.tfsNameBaseCase;

public class RcTfsManager_11_writeFile extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_openWriteFile_then_writeFile() throws IOException
	{
		log.info( "test_01_openWriteFile_then_writeFile" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertEquals(Wret, data.length);
	}
	
	@Test
    public  void  test_02_openWriteFile_then_writeFile_less_length() throws IOException
	{
		log.info( "test_02_openWriteFile_then_writeFile_less_length" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length-10*(1<<10));
		Assert.assertEquals(Wret, data.length-10*(1<<10));
	}
	
	@Test
    public  void  test_03_openWriteFile_then_writeFile_more_length() throws IOException
	{
		log.info( "test_03_openWriteFile_then_writeFile_more_length" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length+1);
		Assert.assertTrue(Wret<0);
	}
	
	@Test
    public  void  test_04_openWriteFile_then_writeFile_with_offset() throws IOException
	{
		log.info( "test_04_openWriteFile_then_writeFile_with_offset" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 10*(1<<10), data.length-30*(1<<10));
		Assert.assertEquals(Wret, data.length-30*(1<<10));
	}
	
	@Test
    public  void  test_05_openWriteFile_then_writeFile_more_offset() throws IOException
	{
		log.info( "test_05_openWriteFile_then_writeFile_more_offset" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, data.length+1,1);
		Assert.assertTrue(Wret<0);
	}
	
	@Test
    public  void  test_06_openWriteFile_then_writeFile_wrong_offset() throws IOException
	{
		log.info( "test_06_openWriteFile_then_writeFile_wrong_offset" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, -1, data.length);
		Assert.assertTrue(Wret<0);
	}
	
	@Test
    public  void  test_07_openWriteFile_then_writeFile_with_wrong_fd() throws IOException
	{
		log.info( "test_07_openWriteFile_then_writeFile_with_wrong_fd" );
		
		int Wret;
		byte []data=null;
		int fd =-1;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertTrue(Wret<0);
	}
	
	@Test
    public  void  test_08_openWriteFile_then_writeFile_with_wrong_fd_2() throws IOException
	{
		log.info( "test_08_openWriteFile_then_writeFile_with_wrong_fd_2" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		tfsManager.closeFile(fd);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertTrue(Wret<0);
	}
	
	@Test
    public  void  test_09_openWriteFile_then_writeFile_with_wrong_fd_3() throws IOException
	{
		log.info( "test_09_openWriteFile_then_writeFile_with_wrong_fd_3" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		Ret=tfsManager.closeFile(fd);
		tfsManager.unlinkFile(Ret, null);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertTrue(Wret<0);
	}
	
	@Test
    public  void  test_10_openReadFile_then_writeFile() throws IOException
	{
		log.info( "test_10_openReadFile_then_writeFile" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertTrue(Wret<0);
	}
	
    public  void  test_11_Large_openWriteFile_then_writeFile() throws IOException
	{
		log.info( "test_11_Large_openWriteFile_then_writeFile" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertEquals(Wret, data.length);
	}
    
	@Test
    public  void  test_12_openWriteFile_then_writeFile_null_data() throws IOException
	{
		log.info( "test_12_openWriteFile_then_writeFile_null_data" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		String buf="";
		data=buf.getBytes();
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertEquals(Wret, data.length);
	}
   
	public  void  test_13_openWriteFile_then_writeFile_large() throws IOException
	{
		log.info( "test_13_openWriteFile_then_writeFile_large" );
		String Ret=null;
		
		int fd=-1;
		fd=tfsManager.openWriteFile(Ret, null, key);
		Assert.assertTrue(fd>0);
		
		int Wret;
		byte []data=null;
		data=getByte(resourcesPath+"5M.jpg");
		Wret=tfsManager.writeFile(fd, data, 0, data.length);
		Assert.assertEquals(Wret, data.length);
	}
}
