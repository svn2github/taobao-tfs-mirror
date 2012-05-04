package com.taobao.common.tfs.testcase.api.rc;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;

public class SaveFileByteTest extends BaseCase {
	@Test
    public  void  test_01_saveFile_byte() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[] = FileUtility.getByte(resourcesPath+"100K.jpg");
		
		String tfsName = tfsManager.saveFile(null,null,data,0,100*(1<<10),false);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_02_saveFile_byte_less_length() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[] = FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,0,10*(1<<10),false);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_03_saveFile_byte_more_length() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]= FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName = tfsManager.saveFile(null,null,data,0,200*(1<<10),false);
		Assert.assertNull(tfsName);
		
		log.info("begin: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_04_saveFile_byte_wrong_offset() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[] = FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,-1,100*(1<<10),false);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_05_saveFile_byte_with_offset_and_length() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,10*(1<<10),60*(1<<10),false);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_06_saveFile_byte_with_suffix() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,".jpg",data,0,100*(1<<10),false);
		log.info("The tfs file name is "+ tfsName);
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_07_saveFile_byte_with_empty_suffix() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,"",data,0,100*(1<<10),false);
		tfsNames.add(tfsName);
		log.info("The tfs file name is "+tfsName);
		Assert.assertNotNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_08_saveFile_byte_with_empty_data() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]="".getBytes();
		String tfsName=tfsManager.saveFile(null,null,data,0,0,false);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_09_saveFile_byte_with_null_data() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile(null,null,null,0,0,false);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_10_saveFile_byte_with_wrong_length() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[] = FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,0,-1,false);
		Assert.assertNull(tfsName);
		
		log.info("begin: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_11_saveFile_byte_with_tfsname() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName = tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean unlinkResult;
		unlinkResult=tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(unlinkResult);
		
		byte data[] = FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName1 = tfsManager.saveFile(tfsName,null,data,0,100*(1<<10),false);
		tfsNames.add(tfsName1);
		Assert.assertNotNull(tfsName1);
		System.out.println("The tfs file name is "+ tfsName1);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_12_saveFile_byte_with_tfsname_and_suffix() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		String tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean unlinkResult=tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(unlinkResult);
		
		byte data[] = FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName1 = tfsManager.saveFile(tfsName,".txt",data,0,100*(1<<10),false);
		tfsNames.add(tfsName1);
		Assert.assertNotNull(tfsName1);
		log.info("The tfs file name is "+ tfsName1);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_13_saveFile_byte_with_wrong_tfsname() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName =tfsManager.saveFile("Tis521423695781236",null,data,0,100*(1<<10),false);
		tfsNames.add(tfsName);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_14_saveFile_byte_Large() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"10m.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,0,10*(1<<20),false);
		tfsNames.add(tfsName);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_15_saveFile_byte_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,0,100*(1<<10),true);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_16_saveFile_byte_less_length_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName = tfsManager.saveFile(null,null,data,0,10*(1<<10),true);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_17_saveFile_byte_more_length_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,0,200*(1<<10),true);
		tfsNames.add(tfsName);
		Assert.assertNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_18_saveFile_byte_wrong_offset_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,-1,100*(1<<10),true);
		tfsNames.add(tfsName);
		Assert.assertNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_19_saveFile_byte_with_offset_and_length_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,10*(1<<10),60*(1<<10),true);
		tfsNames.add(tfsName);
		Assert.assertNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		log.info("end: "+getCurrentFunctionName());
		
	}
	
	@Test
    public  void  test_20_saveFile_byte_with_suffix_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,10*(1<<10),60*(1<<10),true);
		tfsNames.add(tfsName);
		Assert.assertNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		log.info("end: "+getCurrentFunctionName());
		
	}
	
	@Test
    public  void  test_21_saveFile_byte_with_empty_suffix_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,"",data,0,100*(1<<10),true);
		tfsNames.add(tfsName);
		Assert.assertNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_22_saveFile_byte_with_empty_data_simpleName() throws IOException
	{	
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[] ="".getBytes();
		String tfsName=tfsManager.saveFile(null,null,data,0,0,true);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_23_saveFile_byte_with_null_data_simpleName() throws IOException
	{	
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[] =null;
		String tfsName=tfsManager.saveFile(null,null,data,0,0,true);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
	
	@Test
    public  void  test_24_saveFile_byte_with_wrong_length_simpleName() throws IOException
	{
		log.info("begin: "+getCurrentFunctionName());
		
		byte data[]=FileUtility.getByte(resourcesPath+"100K.jpg");
		String tfsName=tfsManager.saveFile(null,null,data,0,-1,true);
		Assert.assertNull(tfsName);
		
		log.info("end: "+getCurrentFunctionName());
	}
}
