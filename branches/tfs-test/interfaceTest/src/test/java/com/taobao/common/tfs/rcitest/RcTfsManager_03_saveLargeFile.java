package com.taobao.common.tfs.rcitest;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.tfsNameBaseCase;

public class RcTfsManager_03_saveLargeFile extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_saveLargeFile_small_size()
	{
		log.info( "test_01_saveLargeFile_small_size" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"10m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_02_saveLargeFile_max_small_size()
	{
		log.info( "test_02_saveLargeFile_max_small_size" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"2m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_03_saveLargeFile_large_size()
	{
		log.info( "test_03_saveLargeFile_large_size" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"1g.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_04_saveLargeFile_with_suffix_max_small_size()
	{
		log.info( "test_04_saveLargeFile_with_suffix_max_small_size" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile( resourcesPath+"2m.jpg",null,"jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	

	@Test
    public  void  test_05_saveLargeFile_with_empty_localFile_max_small_size()
	{
		log.info( "test_05_saveLargeFile_with_empty_localFile_max_small_size" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile("",null,null);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_06_saveLargeFile_with_null_localFile_max_small_size()
	{
		log.info( "test_06_saveLargeFile_with_null_localFile_max_small_size" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile(null,null,null);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_07_saveLargeFile_with_wrong_localFile_max_small_size()
	{
		log.info( "test_07_saveLargeFile_with_wrong_localFile_max_small_size" );
		String Ret=null;
		Ret=tfsManager.saveLargeFile("dsfsdfsdefsasa",null,null);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_08_saveLargeFile_max_small_size_with_tfsname_and_suffix()
	{
		log.info( "test_08_saveLargeFile_max_small_size_with_tfsname_and_suffix" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.unlinkFile(Ret, null);
		Assert.assertTrue(Bret);
		
		String name= Ret;
		Ret=tfsManager.saveFile( resourcesPath+"2m.jpg",name,".jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	

}