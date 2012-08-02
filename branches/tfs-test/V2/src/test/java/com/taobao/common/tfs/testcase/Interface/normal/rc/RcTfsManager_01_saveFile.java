package com.taobao.common.tfs.testcase.Interface.normal.rc;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;




public class RcTfsManager_01_saveFile extends rcTfsBaseCase 
{
	@Test
    public  void  test_01_saveFile_with_right_suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		//sleep(10);
	}
	
	@Test
    public  void  test_02_saveFile_with_empty_suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,"",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_03_saveFile_with_tfsname() 
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.unlinkFile(Ret, null);
		Assert.assertTrue(Bret);
		
		//Thread.sleep(2000);
		String name= Ret;
		System.out.println(name);
		String NRet=null;
		NRet=tfsManager.saveFile( resourcesPath+"100K.jpg",name,null,true);
		Assert.assertNotNull(NRet);
		System.out.println("The tfs file name is "+ NRet);	
	}
	
	@Test
    public  void  test_04_saveFile_with_empty_tfsName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg","",null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_05_saveFile_with_suffix()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_06_saveFile_with_empty_localPath()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( "",null,null,false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_07_saveFile_with_null_localPath()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		String name=null;
		Ret=tfsManager.saveFile(name,null,null,false);
		Assert.assertNull(Ret);
	
	}
	
	@Test
    public  void  test_08_saveFile_to_override_exist_file_big()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"10M.jpg",null,null,false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_09_saveFile_to_override_exist_file()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		String name=Ret;
		Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,null,false);
		Assert.assertNotNull(Ret);
		
	}
	
	@Test
    public  void  test_10_saveFile_with_wrong_localPath()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( "shakdhsakdhsdkj",null,null,false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_11_saveFile_with_tfsname_and_suffix()
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
		Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);	
	}
	
	@Test
    public  void  test_12_saveFile_with_wrong_tfsFileName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg","sadhaksjhda",null,false);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_13_saveFile_with_right_suffix_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_14_saveFile_with_empty_suffix_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,"",true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_15_saveFile_with_tfsname_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.unlinkFile(Ret, null);
		Assert.assertTrue(Bret);
		
		String name= Ret;
		Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);	
	}
	
	@Test
    public  void  test_16_saveFile_with_empty_tfsName_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg","",null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_17_saveFile_with_suffix_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	}
	
	@Test
    public  void  test_18_saveFile_with_empty_localPathsuffix_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( "",null,null,true);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_19_saveFile_with_null_localPath_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		String name=null;
		Ret=tfsManager.saveFile(name,null,null,true);
		Assert.assertNull(Ret);
	
	}
	
	@Test
    public  void  test_20_saveFile_to_override_exist_file_big_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"10M.jpg",null,null,true);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_21_saveFile_to_override_exist_file_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		String name=Ret;
		Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,null,false);
		Assert.assertNotNull(Ret);
	}
	
	@Test
    public  void  test_22_saveFile_with_wrong_localPath_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( "shakdhsakdhsdkj",null,null,true);
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_23_saveFile_with_tfsname_and_suffix_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.unlinkFile(Ret, null);
		Assert.assertTrue(Bret);
		
		String name= Ret;
		Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);	
	}
	
	@Test
    public  void  test_24_saveFile_with_wrong_tfsFileName_simpleName()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg","sadhaksjhda",null,true);
		Assert.assertNull(Ret);
	}
	
}