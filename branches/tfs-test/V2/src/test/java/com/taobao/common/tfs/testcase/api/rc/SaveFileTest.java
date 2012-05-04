package com.taobao.common.tfs.testcase.api.rc;

import junit.framework.Assert;


import org.junit.Test;

import com.taobao.common.tfs.testcase.BaseCase;

public class SaveFileTest extends BaseCase {
	
	@Test
    public  void  test_01_saveFile_with_right_suffix()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		tfsName=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,false);
		log.info("saved tfs name is: "+tfsName);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_02_saveFile_with_empty_suffix()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,"",false);
		log.info("saved tfs name is: "+tfsName);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_03_saveFile_with_tfsname()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfsName is "+ tfsName);
		
		boolean unlinkResult=tfsManager.unlinkFile(tfsName, null);
		log.info("unlink result is: "+unlinkResult);
		Assert.assertTrue(unlinkResult);
		
		String tfsName2 = tfsManager.saveFile( resourcesPath+"100K.jpg",tfsName,null,true);
		log.info("The tfsName2 is "+ tfsName2);
		Assert.assertNotNull(tfsName2);
		
		log.info("end: "+getCurrentFunctionName());	
	}
	
	@Test
    public  void  test_04_saveFile_with_empty_tfsName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg","",null,false);
		tfsNames.add(tfsName);
		log.info("The tfs name is: "+tfsName);
		
		Assert.assertNotNull(tfsName);
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_05_saveFile_with_suffix()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		tfsNames.add(tfsName);
		log.info("The tfs name is: "+tfsName);
		Assert.assertNotNull(tfsName);
		
		Assert.assertNotNull(tfsName);
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_06_saveFile_with_empty_localPath()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		tfsName=tfsManager.saveFile( "",null,null,false);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_07_saveFile_with_null_localPath()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		String localFile = null;
		tfsName=tfsManager.saveFile(localFile,null,null,false);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	//@Test ??
    public  void  test_08_saveFile_to_override_exist_file_big()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		String tfsName1 = tfsManager.saveFile( resourcesPath+"10M.jpg",tfsName,null,false);
		Assert.assertNull(tfsName1);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_09_saveFile_to_override_exist_file()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		String tfsName1 = tfsManager.saveFile( resourcesPath+"100K.jpg",tfsName,null,false);
		tfsNames.add(tfsName1);
		Assert.assertNotNull(tfsName1);
		
		log.info("end: "+getCurrentFunctionName());
		
	}
	
	@Test
    public  void  test_10_saveFile_with_wrong_localPath()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		tfsName=tfsManager.saveFile( "shakdhsakdhsdkj",null,null,false);
		
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_11_saveFile_with_tfsname_and_suffix()
	{
		log.info("begin: "+getCurrentFunctionName());
		String tfsName=null;
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean unlinkResult = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(unlinkResult);
		
		String tfsName1= tfsManager.saveFile( resourcesPath+"100K.jpg",tfsName,".jpg",false);
		tfsNames.add(tfsName1);
		Assert.assertNotNull(tfsName1);
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_12_saveFile_with_wrong_tfsFileName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName = tfsManager.saveFile( resourcesPath+"100K.jpg","sadhaksjhda",null,false);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_13_saveFile_with_right_suffix_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName = tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_14_saveFile_with_empty_suffix_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,"",true);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		Assert.assertNotNull(tfsName);
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_15_saveFile_with_tfsname_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName= tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info(("The tfs file name is "+ tfsName));
		
		boolean unlinkResult = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(unlinkResult);
		
		String tfsName1 = tfsManager.saveFile( resourcesPath+"100K.jpg",tfsName,null,false);
		tfsNames.add(tfsName1);
		log.info(("The tfs file name1 is "+ tfsName1));
		Assert.assertNotNull(tfsName1);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_16_saveFile_with_empty_tfsName_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg","",null,true);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_17_saveFile_with_suffix_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",true);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+tfsName);
		
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_18_saveFile_with_empty_localPathsuffix_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName= tfsManager.saveFile( "",null,null,true);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_19_saveFile_with_null_localPath_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=null;
		String localName=null;
		tfsName=tfsManager.saveFile(localName,null,null,true);
		Assert.assertNull(tfsName);
		
		log.info("begin: "+getCurrentFunctionName());
	}
	
	//@Test ??
    public  void  test_20_saveFile_to_override_exist_file_big_simpleName()
	{
		log.info( "test_20_saveFile_to_override_exist_file_big_simpleName" );
		String Ret=null;
		Ret=tfsManager.saveFile( resourcesPath+"10M.jpg",null,null,true);
		if(Ret!=null){
			System.out.println("tfs name: "+Ret);
		}
		Assert.assertNull(Ret);
	}
	
	@Test
    public  void  test_21_saveFile_to_override_exist_file_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		String tfsName1=tfsManager.saveFile( resourcesPath+"100K.jpg",tfsName,null,false);
		tfsNames.add(tfsName1);
		log.info("The tfs file name1 is "+ tfsName);
		Assert.assertNotNull(tfsName1);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_22_saveFile_with_wrong_localPath_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( "shakdhsakdhsdkj",null,null,true);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_23_saveFile_with_tfsname_and_suffix_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName = tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		System.out.println("The tfs file name is "+ tfsName);
		
		boolean unlinkResult = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(unlinkResult);
		
		String tfsName1 = tfsManager.saveFile( resourcesPath+"100K.jpg",tfsName,".jpg",false);
		tfsNames.add(tfsName1);
		Assert.assertNotNull(tfsName1);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_24_saveFile_with_wrong_tfsFileName_simpleName()
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName = tfsManager.saveFile( resourcesPath+"100K.jpg","sadhaksjhda",null,true);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	public void sleep(int sec)
	{
		try
		{
			Thread.sleep(sec*1000);
		} 
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
