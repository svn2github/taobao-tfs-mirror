package com.taobao.common.tfs.testcase.Interface.normal.rc;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;



public class RcTfsManager_10_readFile_fileOffset extends rcTfsBaseCase 
{
	@Test
    public  void  test_01_openReadFile_then_readFile() throws IOException
	{
		log.info( "test_01_openReadFile_then_readFile" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 0,data,0, 5*(1<<20));
		Assert.assertEquals(Rret,5*(1<<20));	
	}
	
	@Test
    public  void  test_02_openReadFile_then_readFile_less_length() throws IOException
	{
		log.info( "test_02_openReadFile_then_readFile_less_length" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 0,data,0, 4*(1<<20));
		Assert.assertEquals(Rret,4*(1<<20));	
	}
	
	@Test
    public  void  test_03_openReadFile_then_readFile_more_length() throws IOException
	{
		log.info( "test_03_openReadFile_then_readFile_more_length" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [6*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 0,data,0, 6*(1<<20));
		System.out.println(Rret);
		Assert.assertEquals(Rret,5*(1<<20));	
	}
	
	@Test
    public  void  test_04_openReadFile_then_readFile_with_offset() throws IOException
	{
		log.info( "test_04_openReadFile_then_readFile_with_offset" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 0,data,1<<10, 4*(1<<20));
		System.out.println(Rret);
		Assert.assertEquals(Rret,4*(1<<20));	
	}
	
	@Test
    public  void  test_05_openReadFile_then_readFile_more_offset() throws IOException
	{
		log.info( "test_05_openReadFile_then_readFile_more_offset" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [6*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 0,data,5*(1<<20), 10);
		System.out.println(Rret);
		Assert.assertEquals(Rret,10);	
	}
	
	@Test
    public  void  test_06_openReadFile_then_readFile_wrong_offset() throws IOException
	{
		log.info( "test_06_openReadFile_then_readFile_wrong_offset" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 0,data,-1, 5*(1<<20));
		Assert.assertTrue(Rret<0);	
	}
	
	@Test
    public  void  test_07_openReadFile_then_readFile_more_offset_and_length() throws IOException
	{
		log.info( "test_07_openReadFile_then_readFile_more_offset_and_length" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [10*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 0,data,5*(1<<20), 5*(1<<20));
		Assert.assertEquals(Rret,5*(1<<20));	
	}
	
	@Test
    public  void  test_08_openReadFile_then_readFile_with_wrong_fd() throws IOException
	{
		log.info( "test_08_openReadFile_then_readFile_with_wrong_fd" );
	
		int fd=-1;
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd,0, data,0, 5*(1<<20));
		Assert.assertTrue(Rret<0);
		
	}
	
	@Test
    public  void  test_09_openReadFile_then_readFile_with_wrong_fd_2() throws IOException
	{
		log.info( "test_09_openReadFile_then_readFile_with_wrong_fd_2" );
	
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
		data = new byte [5*(1<<20)];
		
		Rret=tfsManager.readFile(fd, 0,data,0, 5*(1<<20));
		Assert.assertTrue(Rret<0);
	}
	
	@Test
    public  void  test_10_openReadFile_then_readFile_with_wrong_fd_3() throws IOException
	{
		log.info( "test_10_openReadFile_then_readFile_with_wrong_fd_3" );
	
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
		data = new byte [5*(1<<20)];
		
		Rret=tfsManager.readFile(fd, 0,data,0, 5*(1<<20));
		Assert.assertTrue(Rret<0);
		
	}
	
	@Test
    public  void  test_11_openReadFile_then_readFile_with_fileOffset() throws IOException
	{
		log.info( "test_11_openReadFile_then_readFile_with_fileOffset" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 10*(1<<10),data,0, 5*(1<<20));
		Assert.assertEquals(Rret,5*(1<<20)-10*(1<<10));	
	}
	
	@Test
    public  void  test_12_openReadFile_then_readFile_with_fileOffset_with_offset() throws IOException
	{
		log.info( "test_12_openReadFile_then_readFile_with_fileOffset_with_offset" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 10*(1<<10),data,10*(1<<10), 4*(1<<20));
		System.out.println(Rret);
		Assert.assertEquals(Rret,4*(1<<20));	
	}
	
	@Test
    public  void  test_13_openReadFile_then_readFile_with_more_fileOffset() throws IOException
	{
		log.info( "test_13_openReadFile_then_readFile_with_more_fileOffset" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		data = new byte [5*(1<<20)];
		int Rret;
		Rret=tfsManager.readFile(fd, 5*(1<<20)+1,data,0,1);
		Assert.assertTrue(Rret==0);	
	}
	
	@Test
    public  void  test_14_openReadFile_then_readFile_with_wrong_fileOffset() throws IOException
	{
		log.info( "test_14_openReadFile_then_readFile_with_wrong_fileOffset" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		byte[] data=null;
		int Rret;
		Rret=tfsManager.readFile(fd, -1,data,0,5*(1<<20));
		Assert.assertTrue(Rret<0);	
	}
	
	@Test
    public  void  test_15_openReadFile_then_readFile_null_data() throws IOException
	{
		log.info( "test_15_openReadFile_then_readFile_null_data" );
		String Ret=null;

		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		int fd=-1;
		fd=tfsManager.openReadFile(Ret, null);
		Assert.assertTrue(fd>0);
		
		int Rret;
		Rret=tfsManager.readFile(fd, 0,null,0, 5*(1<<20));
		Assert.assertTrue(Rret<0);
	}
}