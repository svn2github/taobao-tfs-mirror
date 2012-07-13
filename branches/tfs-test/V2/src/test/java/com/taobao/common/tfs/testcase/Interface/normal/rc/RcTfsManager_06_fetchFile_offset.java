package com.taobao.common.tfs.testcase.Interface.normal.rc;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;



public class RcTfsManager_06_fetchFile_offset extends rcTfsBaseCase 
{
	@Test
    public  void  test_01_saveFile_then_fetchFile_offset_with_right_suffix() throws FileNotFoundException
	{
		log.info( "test_01_saveFile_then_fetchFile_offset_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_02_saveFile_then_fetchFile_offset_with_wrong_suffix() throws FileNotFoundException
	{
		log.info( "test_02_saveFile_then_fetchFile_offset_with_wrong_suffix" );
		String Ret=null;

		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_03_saveFile_then_fetchFile_offset_with_empty_suffix() throws FileNotFoundException
	{
		log.info( "test_03_saveFile_then_fetchFile_offset_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_04_saveFile_then_fetchFile_offset_with_wrong_tfsName() throws FileNotFoundException
	{
		log.info( "test_04_saveFile_then_fetchFile_offset_with_wrong_tfsName" );

		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		
		boolean Bret;
		Bret=tfsManager.fetchFile("hsadsjkadhjksal", null,0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_05_saveFile_then_fetchFile_offset_with_empty_tfsName() throws FileNotFoundException
	{
		log.info( "test_05_saveFile_then_fetchFile_offset_with_empty_tfsName" );

		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		
		boolean Bret;
		Bret=tfsManager.fetchFile("", null,0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_06_saveFile_then_fetchFile_offset_with_null_tfsName_suffix() throws FileNotFoundException
	{
		log.info( "test_06_saveFile_then_fetchFile_offset_with_null_tfsName_suffix" );

		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		
		boolean Bret;
		Bret=tfsManager.fetchFile(null, null,0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_07_saveFile_then_fetchFile_offset_with_null_OutputStream() throws FileNotFoundException
	{
		log.info( "test_07_saveFile_then_fetchFile_offset_with_null_OutputStream" );
		String Ret=null;

		long count=100*(1<<10);
		 
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,null);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_08_saveFile_with_suffix_then_fetchFile_offset() throws FileNotFoundException
	{
		log.info( "test_08_saveFile_with_suffix_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_09_saveFile_with_suffix_and_name_then_fetchFile_offset() throws FileNotFoundException
	{
		log.info( "test_09_saveFile_with_suffix_and_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,".jpg",false);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_10_saveFile_with_name_then_fetchFile_offset() throws FileNotFoundException
	{
		log.info( "test_10_saveFile_with_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,null,false);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_11_saveFile_simpleName_then_fetchFile_offset_with_right_suffix() throws FileNotFoundException
	{
		log.info( "test_11_saveFile_simpleName_then_fetchFile_offset_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_12_saveFile_simpleName_then_fetchFile_offset_with_wrong_suffix() throws FileNotFoundException
	{
		log.info( "test_12_saveFile_simpleName_then_fetchFile_offset_with_wrong_suffix" );
		String Ret=null;

		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_13_saveFile_simpleName_then_fetchFile_offset_with_empty_suffix() throws FileNotFoundException
	{
		log.info( "test_13_saveFile_simpleName_then_fetchFile_offset_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_14_saveFile_simpleName_with_suffix_and_name_then_fetchFile_offset() throws FileNotFoundException
	{
		log.info( "test_14_saveFile_simpleName_with_suffix_and_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,".jpg",true);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_15_saveFile_simpleName_with_suffix_then_fetchFile_offset() throws FileNotFoundException
	{
		log.info( "test_15_saveFile_simpleName_with_suffix_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_16_saveFile_simpleName_with_name_then_fetchFile_offset() throws FileNotFoundException
	{
		log.info( "test_16_saveFile_simpleName_with_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",name,".jpg",true);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_17_saveFile_byte_then_fetchFile_offset_with_right_suffix() throws IOException
	{
		log.info( "test_17_saveFile_byte_then_fetchFile_offset_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_18_saveFile_byte_then_fetchFile_offset_with_wrong_suffix() throws IOException
	{
		log.info( "test_18_saveFile_byte_then_fetchFile_offset_with_wrong_suffix" );
		String Ret=null;

		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_19_saveFile_byte_then_fetchFile_offset_with_empty_suffix() throws IOException
	{
		log.info( "test_19_saveFile_byte_then_fetchFile_offset_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_20_saveFile_byte_with_suffix_and_name_then_fetchFile_offset() throws IOException
	{
		log.info( "test_20_saveFile_byte_with_suffix_and_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(name,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_21_saveFile_byte_with_suffix_then_fetchFile_offset() throws IOException
	{
		log.info( "test_21_saveFile_byte_with_suffix_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_22_saveFile_byte_with_name_then_fetchFile_offset() throws IOException
	{
		log.info( "test_22_saveFile_byte_with_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(name,null,data,0,100*(1<<10),false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_23_saveFile_byte_simpleName_then_fetchFile_offset_with_right_suffix() throws IOException
	{
		log.info( "test_23_saveFile_byte_simpleName_then_fetchFile_offset_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_24_saveFile_byte_simpleName_then_fetchFile_offset_with_wrong_suffix() throws IOException
	{
		log.info( "test_24_saveFile_byte_simpleName_then_fetchFile_offset_with_wrong_suffix" );
		String Ret=null;

		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_25_saveFile_byte_simpleName_then_fetchFile_offset_with_empty_suffix() throws IOException
	{
		log.info( "test_25_saveFile_byte_simpleName_then_fetchFile_offset_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_26_saveFile_byte_simpleName_with_suffix_and_name_then_fetchFile_offset() throws IOException
	{
		log.info( "test_26_saveFile_byte_simpleName_with_suffix_and_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(name,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_27_saveFile_byte_simpleName_with_suffix_then_fetchFile_offset() throws IOException
	{
		log.info( "test_27_saveFile_byte_simpleName_with_suffix_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_28_saveFile_byte_with_name_then_fetchFile_offset() throws IOException
	{
		log.info( "test_28_saveFile_byte_with_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"100K.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"100K.jpg");
		Ret=tfsManager.saveFile(name,null,data,0,100*(1<<10),true);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_29_saveLargeFile_then_fetchFile_offset_with_right_suffix() throws FileNotFoundException
	{
		log.info( "test_29_saveLargeFile_then_fetchFile_offset_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_30_saveLargeFile_then_fetchFile_offset_with_wrong_suffix() throws FileNotFoundException
	{
		log.info( "test_30_saveLargeFile_then_fetchFile_offset_with_wrong_suffix" );
		String Ret=null;

		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_31_saveLargeFile_then_fetchFile_offset_with_empty_suffix() throws FileNotFoundException
	{
		log.info( "test_31_saveLargeFile_then_fetchFile_offset_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	
    public  void  test_32_saveLargeFile_with_suffix_and_name_then_fetchFile_offset() throws FileNotFoundException
	{
		log.info( "test_32_saveLargeFile_with_suffix_and_name_then_fetchFile_offset" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",name,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_33_saveLargeFile_with_suffix_then_fetchFile() throws FileNotFoundException
	{
		log.info( "test_33_saveLargeFile_with_suffix_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",null,".jpg");
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	
    public  void  test_34_saveLargeFile_with_name_then_fetchFile() throws FileNotFoundException
	{
		log.info( "test_34_saveLargeFile_with_name_then_fetchFile" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		Ret=tfsManager.saveLargeFile( resourcesPath+"5M.jpg",name,null);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_35_saveLargeFile_byte_then_fetchFile_byte_with_right_suffix() throws IOException
	{
		log.info( "test_35_saveLargeFile_byte_then_fetchFile_byte_with_right_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_36_saveLargeFile_byte_then_fetchFile_byte_with_wrong_suffix() throws IOException
	{
		log.info( "test_36_saveLargeFile_byte_then_fetchFile_byte_with_wrong_suffix" );
		String Ret=null;

		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_37_saveLargeFile_byte_then_fetchFile_byte_with_empty_suffix() throws IOException
	{
		log.info( "test_37_saveLargeFile_byte_then_fetchFile_byte_with_empty_suffix" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, "",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	
    public  void  test_38_saveLargeFile_byte_with_suffix_and_name_then_fetchFile_byte() throws IOException
	{
		log.info( "test_38_saveLargeFile_byte_with_suffix_and_name_then_fetchFile_byte" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(name,".jpg",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_39_saveLargeFile_byte_with_suffix_then_fetchFile_byte() throws IOException
	{
		log.info( "test_39_saveLargeFile_byte_with_suffix_then_fetchFile_byte" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(null,".jpg",data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, ".jpg",0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	
    public  void  test_40_saveLargeFile_byte_with_name_then_fetchFile_byte() throws IOException
	{
		log.info( "test_40_saveLargeFile_byte_with_name_then_fetchFile_byte" );
		String Ret=null;
		int savecrc;
		int fetchcrc;
		long count=10*(1<<20);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		savecrc=FileUtility.getCrc(resourcesPath+"5M.jpg");
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,".jpg",false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		tfsManager.unlinkFile(Ret, null);
		String name=Ret ;
		
		byte data[]=null;
		data=FileUtility.getByte(resourcesPath+"5M.jpg");
		Ret=tfsManager.saveLargeFile(name,null,data,0,data.length,key);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,0,count,output);
		Assert.assertTrue(Bret);
		fetchcrc=FileUtility.getCrc(resourcesPath+"empty.jpg");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
    public  void  test_41_saveFile_then_fetchFile_offset_with_wrong_fileOffset() throws FileNotFoundException
	{
		log.info( "test_41_saveFile_then_fetchFile_offset_with_wrong_fileOffset" );
		String Ret=null;
		long count=100*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,-1,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_42_saveFile_then_fetchFile_offset_more_fileOffset_and_length() throws FileNotFoundException
	{
		log.info( "test_42_saveFile_then_fetchFile_offset_more_fileOffset_and_length" );
		String Ret=null;
		long count=1;
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,100*(1<<10)+1,count,output);
		Assert.assertFalse(Bret);
	}
	
	@Test
    public  void  test_43_saveFile_then_fetchFile_offset_with_more_fileoffset() throws FileNotFoundException
	{
		log.info( "test_43_saveFile_then_fetchFile_offset_with_more_fileoffset" );
		String Ret=null;
		long count=50*(1<<10);
		OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
		 
		
		Ret=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(Ret);
		System.out.println("The tfs file name is "+ Ret);
		
		boolean Bret;
		Bret=tfsManager.fetchFile(Ret, null,60*(1<<10),count,output);
		Assert.assertTrue(Bret);
	}
}