package com.taobao.common.tfs.rcitest;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.tfsNameBaseCase;

public class RcTfsManager_05_fetchFile extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_saveFile_then_fetchFile_with_right_suffix()
	{
		log.info( "test_01_saveFile_then_fetchFile_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,false);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_02_saveFile_then_fetchFile_with_wrong_suffix()
	{
		log.info( "test_02_saveFile_then_fetchFile_with_wrong_suffix" );
		String Ret=null;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_03_saveFile_then_fetchFile_with_empty_suffix()
	{
		log.info( "test_03_saveFile_then_fetchFile_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,false);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_04_saveFile_then_fetchFile_with_wrong_tfsName()
	{
		log.info( "test_04_saveFile_then_fetchFile_with_wrong_tfsName" );
		
		boolean Bret;
		Bret=tfsManager.fetchFile("jhskahdkwahskdsajk", ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_05_saveFile_then_fetchFile_with_empty_tfsName()
	{
		log.info( "test_05_saveFile_then_fetchFile_with_empty_tfsName" );
		
		boolean Bret;
		Bret=tfsManager.fetchFile("", ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_06_saveFile_then_fetchFile_with_null_tfsName_suffix()
	{
		log.info( "test_05_saveFile_then_fetchFile_with_empty_tfsName" );
		
		boolean Bret;
		Bret=tfsManager.fetchFile(null, ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_07_saveFile_then_fetchFile_with_empty_localPath()
	{
		log.info( "test_07_saveFile_then_fetchFile_with_empty_localPath" );
		String Ret=null;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", "");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_08_saveFile_then_fetchFile_with_null_localPath()
	{
		log.info( "test_08_saveFile_then_fetchFile_with_null_localPath" );
		String Ret=null;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		String name=null;
		Bret=tfsManager.fetchFile(Ret, ".jpg", name);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_09_saveFile_with_suffix_and_name_then_fetchFile()
	{
		log.info( "test_09_saveFile_with_suffix_and_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,".jpg",false);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",name,null,true);
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_10_saveFile_with_suffix_then_fetchFile()
	{
		log.info( "test_10_saveFile_with_suffix_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,".jpg",false);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_11_saveFile_with_name_then_fetchFile()
	{
		log.info( "test_11_saveFile_with_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		boolean Bret;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,".jpg",false);
	
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file 1 name is "+ Ret);
		String name =Ret;
		
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);	
		
		
		Bret=tfsManager.unlinkFile(Ret, null);
		Assert.assertTrue(Bret);
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",name,null,true);
		System.out.println("The tfs file 2 name is "+ Ret);
		
		
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_12_saveFile_simpleName_then_fetchFile_with_right_suffix()
	{
		log.info( "test_12_saveFile_simpleName_then_fetchFile_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_13_saveFile_simpleName_then_fetchFile_with_wrong_suffix()
	{
		log.info( "test_13_saveFile_simpleName_then_fetchFile_with_wrong_suffix" );
		String Ret=null;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_14_saveFile_simpleName_then_fetchFile_with_empty_suffix()
	{
		log.info( "test_14_saveFile_simpleName_then_fetchFile_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_15_saveFile_simpleName_then_fetchFile_with_empty_localPath()
	{
		log.info( "test_15_saveFile_simpleName_then_fetchFile_with_empty_localPath" );
		String Ret=null;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", "");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_16_saveFile_simpleName_then_fetchFile_with_null_localPath()
	{
		log.info( "test_16_saveFile_simpleName_then_fetchFile_with_null_localPath" );
		String Ret=null;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		String name=null;
		Bret=tfsManager.fetchFile(Ret, ".jpg", name);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_17_saveFile_simpleName_with_suffix_and_name_then_fetchFile()
	{
		log.info( "test_17_saveFile_simpleName_with_suffix_and_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",name,".jpg",true);
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_18_saveFile_simpleName_with_suffix_then_fetchFile()
	{
		log.info( "test_18_saveFile_simpleName_with_suffix_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,".jpg",true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_19_saveFile_simpleName_with_name_then_fetchFile()
	{
		log.info( "test_19_saveFile_simpleName_with_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",name,null,true);
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_20_saveFile_byte_then_fetchFile_with_right_suffix() throws IOException
	{
		log.info( "test_20_saveFile_byte_then_fetchFile_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"100k.jpg");
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_21_saveFile_byte_then_fetchFile_with_wrong_suffix() throws IOException
	{
		log.info( "test_21_saveFile_byte_then_fetchFile_with_wrong_suffix" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
	
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_22_saveFile_byte_then_fetchFile_with_empty_suffix() throws IOException
	{
		log.info( "test_22_saveFile_byte_then_fetchFile_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"100k.jpg");
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_23_saveFile_byte_then_fetchFile_with_empty_localPath() throws IOException
	{
		log.info( "test_23_saveFile_byte_then_fetchFile_with_empty_localPath" );
		String Ret=null;
		
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", "");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_24_saveFile_byte_then_fetchFile_with_null_localPath() throws IOException
	{
		log.info( "test_24_saveFile_byte_then_fetchFile_with_null_localPath" );
		String Ret=null;
		
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		String name=null;
		Bret=tfsManager.fetchFile(Ret, ".jpg", name);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_25_saveFile_byte_with_suffix_and_name_then_fetchFile() throws IOException
	{
		log.info( "test_25_saveFile_byte_with_suffix_and_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(name,".jpg",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_26_saveFile_byte_with_suffix_then_fetchFile() throws IOException
	{
		log.info( "test_26_saveFile_byte_with_suffix_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"100k.jpg");
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_27_saveFile_byte_with_name_then_fetchFile() throws IOException
	{
		log.info( "test_27_saveFile_byte_with_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(name,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_28_saveFile_byte_simpleName_then_fetchFile_with_right_suffix() throws IOException
	{
		log.info( "test_28_saveFile_byte_simpleName_then_fetchFile_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"100k.jpg");
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_29_saveFile_byte_simpleName_then_fetchFile_with_wrong_suffix() throws IOException
	{
		log.info( "test_29_saveFile_byte_simpleName_then_fetchFile_with_wrong_suffix" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
	
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_30_saveFile_byte_simpleName_then_fetchFile_with_empty_suffix() throws IOException
	{
		log.info( "test_30_saveFile_byte_simpleName_then_fetchFile_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"100k.jpg");
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_31_saveFile_byte_simpleName_then_fetchFile_with_empty_localPath() throws IOException
	{
		log.info( "test_31_saveFile_byte_simpleName_then_fetchFile_with_empty_localPath" );
		String Ret=null;
		
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", "");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_32_saveFile_byte_simpleName_then_fetchFile_with_null_localPath() throws IOException
	{
		log.info( "test_32_saveFile_byte_simpleName_then_fetchFile_with_null_localPath" );
		String Ret=null;
		
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		String name=null;
		Bret=tfsManager.fetchFile(Ret, ".jpg", name);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_33_saveFile_byte_simpleName_with_suffix_and_name_then_fetchFile() throws IOException
	{
		log.info( "test_33_saveFile_byte_simpleName_with_suffix_and_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(name,".jpg",data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_34_saveFile_byte_simpleName_with_suffix_then_fetchFile() throws IOException
	{
		log.info( "test_34_saveFile_byte_simpleName_with_suffix_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"100k.jpg");
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_35_saveFile_byte_simpleName_with_name_then_fetchFile() throws IOException
	{
		log.info( "test_35_saveFile_byte_simpleName_with_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		byte data[]=null;
		data=getByte(resourcesPath+"100k.jpg");
		Ret=tfsManager.saveFile(name,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_36_saveLargeFile_then_fetchFile_with_right_suffix()
	{
		log.info( "test_36_saveLargeFile_then_fetchFile_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_37_saveLargeFile_then_fetchFile_with_wrong_suffix()
	{
		log.info( "test_37_saveLargeFile_then_fetchFile_with_wrong_suffix" );
		String Ret=null;
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_38_saveLargeFile_then_fetchFile_with_empty_suffix()
	{
		log.info( "test_38_saveLargeFile_then_fetchFile_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		savecrc=getCrc(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);;
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_39_saveLargeFile_then_fetchFile_with_empty_localPath()
	{
		log.info( "test_39_saveLargeFile_then_fetchFile_with_empty_localPath" );
		String Ret=null;
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", "");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_40_saveLargeFile_then_fetchFile_with_null_localPath()
	{
		log.info( "test_40_saveLargeFile_then_fetchFile_with_null_localPath" );
		String Ret=null;
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		String name=null;
		Bret=tfsManager.fetchFile(Ret, ".jpg", name);
		Assert.assertFalse(Bret);
	}
	
	
    public  void  test_41_saveLargeFile_with_suffix_and_name_then_fetchFile()
	{
		log.info( "test_41_saveLargeFile_with_suffix_and_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",name,".jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);;
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_42_saveLargeFile_with_suffix_then_fetchFile()
	{
		log.info( "test_42_saveLargeFile_with_suffix_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		savecrc=getCrc(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",null,".jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);;
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	
    public  void  test_43_saveLargeFile_with_name_then_fetchFile()
	{
		log.info( "test_43_saveLargeFile_with_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5m.jpg",name,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_44_saveLargeFile_byte_then_fetchFile_with_right_suffix() throws IOException
	{
		log.info( "test_44_saveLargeFile_byte_then_fetchFile_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"5m.jpg");
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_45_saveLargeFile_byte_then_fetchFile_with_wrong_suffix() throws IOException
	{
		log.info( "test_45_saveLargeFile_byte_then_fetchFile_with_wrong_suffix" );
		String Ret=null;
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
	
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_46_saveLargeFile_byte_then_fetchFile_with_empty_suffix() throws IOException
	{
		log.info( "test_46_saveLargeFile_byte_then_fetchFile_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"5m.jpg");
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_47_saveLargeFile_byte_then_fetchFile_with_empty_localPath() throws IOException
	{
		log.info( "test_47_saveLargeFile_byte_then_fetchFile_with_empty_localPath" );
		String Ret=null;
		
		
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", "");
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_48_saveLargeFile_byte_then_fetchFile_with_null_localPath() throws IOException
	{
		log.info( "test_48_saveLargeFile_byte_then_fetchFile_with_null_localPath" );
		String Ret=null;
		
		
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		String name=null;
		Bret=tfsManager.fetchFile(Ret, ".jpg", name);
		Assert.assertFalse(Bret);
	}
	
	
    public  void  test_49_saveLargeFile_byte_with_suffix_and_name_then_fetchFile() throws IOException
	{
		log.info( "test_49_saveLargeFile_byte_with_suffix_and_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(name,".jpg",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_50_saveFile_byte_with_suffix_then_fetchFile() throws IOException
	{
		log.info( "test_50_saveFile_byte_with_suffix_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		
		savecrc=getCrc(resourcesPath+"5m.jpg");
		
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(null,".jpg",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg", resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	
    public  void  test_51_saveLargeFile_byte_with_name_then_fetchFile() throws IOException
	{
		log.info( "test_51_saveLargeFile_byte_with_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		
		Ret=tfsManager.saveFile( resourcesPath+"100k.jpg",null,null,true);
		savecrc=getCrc(resourcesPath+"100k.jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		String name =Ret;
		
		tfsManager.unlinkFile(Ret, null);
		
		byte data[]=null;
		data=getByte(resourcesPath+"5m.jpg");
		Ret=tfsManager.saveLargeFile(name,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null, resourcesPath+"TEMP");
		Assert.assertTrue(Bret);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
}