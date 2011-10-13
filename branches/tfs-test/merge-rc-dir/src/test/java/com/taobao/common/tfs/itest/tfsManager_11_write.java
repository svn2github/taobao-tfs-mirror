package com.taobao.common.tfs.itest;


import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.function.tfsNameBaseCase;


public class tfsManager_11_write extends tfsNameBaseCase 
{
	@Test
    public void test_01_write_right() throws IOException 
	{
	   log.info("test_01_write_right");
	   tfsManager.createFile( userId, "/text"); 
	   int localCrc = getCrc(resourcesPath+"10K"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   Assert.assertNotNull(data);
	   long len = data.length;
	   long  offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,len);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   int TfsCrc = getCrc(resourcesPath+"temp");
	   Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test() 
	{
	   boolean ret = tfsManager.createFile( userId, "/text1"); 
	   Assert.assertTrue(ret);
       String s = "test for test";
       long length = s.length();
       byte[] data = s.getBytes();
       Assert.assertNotNull(data);
       long lret = tfsManager.write( userId, "/text1", 0, data, 0, length);
       Assert.assertEquals(lret, length);
	
    }
	@Test
    public void test_02_write_more_offset() throws IOException 
	{
	   log.info("test_02_write_more_offset");
	   tfsManager.createFile( userId, "/text"); 
	   int localCrc = getCrc(resourcesPath+"10K"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = data.length;
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,10*(1<<10));
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   int TfsCrc = getCrc(resourcesPath+"temp");
	   Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_03_write_n_1_offset() throws IOException 
	{
	   log.info("test_03_write_n_1_offset");
	   tfsManager.createFile( userId, "/text"); 
	   int localCrc = getCrc(resourcesPath+"10K"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=-1;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,10*(1<<10));
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   int TfsCrc = getCrc(resourcesPath+"temp");
	   Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_04_write_wrong_offset() throws IOException 
	{
	   log.info("test_04_write_wrong_offset");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=-2;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   //Assert.assertNotEquals(Ret,10*(1<<10));
	   Assert.assertFalse(Ret>=0);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_05_write_with_dataOffset() throws IOException 
	{
	   log.info("test_05_write_with_dataOffset");
	   tfsManager.createFile( userId, "/text");  
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 9*(1<<10);
	   long offset=0;
	   long dataOffset=1023;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   System.out.println(Ret);
	   Assert.assertEquals(Ret,9*(1<<10));
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_06_write_more_dataOffset() throws IOException 
	{
	   log.info("test_06_write_more_dataOffset");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=10*(1<<10)+100;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertTrue(Ret<0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   //Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_07_write_wrong_dataOffset() throws IOException 
	{
	   log.info("test_07_write_wrong_dataOffset");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=-1;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertFalse(Ret>=0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_08_write_null_data() throws IOException 
	{
	   log.info("test_07_write_wrong_dataOffset");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   long len = 0;
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertTrue(Ret<0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   //Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_09_write_empty_data() 
	{
	   log.info("test_09_write_empty_data");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   long len = 0;
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertTrue(Ret<0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   //Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_10_write_null_filename() throws IOException 
	{
	   log.info("test_10_write_null_filename");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, null, offset,data, dataOffset, len);
	   Assert.assertFalse(Ret>=0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   //Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_11_write_empty_filename() throws IOException 
	{
	   log.info("test_11_write_empty_filename");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "", offset,data, dataOffset, len);
	   Assert.assertFalse(Ret>=0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   //Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_12_write_wrong_filename_1() throws IOException 
	{
	   log.info("test_12_write_wrong_filename_1");
	   tfsManager.createFile( userId, "/text"); 
	   int localCrc = getCrc(resourcesPath+"10K"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "///text///", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,10*(1<<10));
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   int TfsCrc = getCrc(resourcesPath+"10K");
	   Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_13_write_wrong_filename_2() throws IOException 
	{
	   log.info("test_13_write_wrong_filename_2");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "text", offset,data, dataOffset, len);
	   Assert.assertFalse(Ret>=0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   //Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_14_write_wrong_filename_3() throws IOException 
	{
	   log.info("test_14_write_wrong_filename_3");
	   tfsManager.createFile( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/", offset,data, dataOffset, len);
	   Assert.assertFalse(Ret>=0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   //Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_15_write_Dir() throws IOException 
	{
	   log.info("test_15_write_Dir");
	   tfsManager.createDir( userId, "/text"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertFalse(Ret>0);
	   tfsManager.rmDir( userId, "/text");
    }
	@Test
    public void test_16_write_many_times() throws IOException 
	{
	   log.info("test_16_write_many_times");
	   tfsManager.createFile( userId, "/text"); 
	   int localCrc = getCrc(resourcesPath+"10K"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,10*(1<<10));
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   System.out.println(Ret);
	   Assert.assertTrue(Ret==0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   int TfsCrc = getCrc(resourcesPath+"10K");
	   Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_17_write_not_exist() throws IOException 
	{
	   log.info("test_17_write_not_exist"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/test", offset,data, dataOffset, len);
	   System.out.println("!!!!!!!!!!!!!!!!!!!"+Ret);
	   Assert.assertFalse(Ret==0);
	   tfsManager.rmFile( userId, "/text");
    }
	//@Test
    public void test_18_write_large() throws IOException 
	{
	   log.info("test_18_write_large");
	   tfsManager.createFile( userId, "/text"); 
	   int localCrc = getCrc(resourcesPath+"1G"); 
	   byte data[]=null;
	   data=getByte(resourcesPath+"10K");
	   long len = 1<<30;
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,1<<30);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   int TfsCrc = getCrc(resourcesPath+"1G");
	   Assert.assertEquals(TfsCrc,localCrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_19_write_many_times_parts() throws IOException 
	{
	   log.info("test_19_write_many_times_parts");
	   tfsManager.createFile( userId, "/text"); 
       byte data[]=null;
       data=getByte(resourcesPath+"10K");
	   long len = 10*(1<<10);
	   long offset=0;
	   long dataOffset=0;
	   long  Ret;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,10*(1<<10));
	   
	   len = 10*(1<<10);
	   offset=10*(1<<10);
	   dataOffset=0;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,10*(1<<10));
	   
	   len = 10*(1<<10);
	   offset=20*(1<<10)+1;
	   dataOffset=0;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,10*(1<<10));
	   
	   data=getByte(resourcesPath+"1b");
	   len = 1;
	   offset=20*(1<<10);
	   dataOffset=0;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertEquals(Ret,1);
	   
	   data=getByte(resourcesPath+"1b");
	   len = 1;
	   offset=20*(1<<10);
	   dataOffset=0;
	   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
	   Assert.assertTrue(Ret==0);
	   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
	   tfsManager.rmFile( userId, "/text");
    }
	 @Test
	 public void test_20_write_large_many_times_parts() throws IOException 
		{
		   log.info("test_20_write_large_many_times_parts");
		   tfsManager.createFile( userId, "/text"); 
	       byte data[]=null;
	       data=getByte(resourcesPath+"3M");
		   long len = 3*(1<<20);
		   long offset=0;
		   long dataOffset=0;
		   long  Ret;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertEquals(Ret,3*(1<<20));
		   
		   len = 3*(1<<20);
		   offset=3*(1<<20);
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertEquals(Ret,3*(1<<20));
		   
		   len = 3*(1<<20);
		   offset=6*(1<<20)+1;
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertEquals(Ret,3*(1<<20));
		   
		   data=getByte(resourcesPath+"1b");
		   Assert.assertNotNull(data);
		   len = 1;
		   offset=6*(1<<20);
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+Ret);
		   Assert.assertEquals(Ret,1);
		   //ret ·µ»ØÊ±0
		   
		   data=getByte(resourcesPath+"1b");
		   len = 1;
		   offset=3*(1<<20);
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertTrue(Ret==0);
		   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
		   tfsManager.rmFile( userId, "/text");
	    }
	 @Test
	 public void test_21_write_many_times_parts_com() throws IOException 
		{
		   log.info("test_21_write_many_times_parts_com");
		   tfsManager.createFile( userId, "/text"); 
	       byte data[]=null;
	       data=getByte(resourcesPath+"10K");
		   long len = 10*(1<<10);
		   long offset=0;
		   long dataOffset=0;
		   long  Ret;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertEquals(Ret,10*(1<<10));
		   
		   data=getByte(resourcesPath+"2M");
		   len = 2*(1<<20);
		   offset=20*(1<<10);
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+Ret);
		   Assert.assertEquals(Ret,2*(1<<20));
		   
		   data=getByte(resourcesPath+"3M");
		   len = 3*(1<<20);
		   offset=8*(1<<20)+20*(1<<10);
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertEquals(Ret,3*(1<<20));
		   
		   data=getByte(resourcesPath+"10K");
		   len = 10*(1<<10);
		   offset=10*(1<<10);
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertEquals(Ret,10*(1<<10));
		   
		   data=getByte(resourcesPath+"10K");
		   len = 10*(1<<10);
		   offset=0;
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+Ret);
		   Assert.assertTrue(Ret==0);
		   
		   data=getByte(resourcesPath+"3M");
		   len = 3*(1<<20);
		   offset=2*(1<<20)+20*(1<<10);
		   dataOffset=0;
		   Ret=tfsManager.write( userId, "/text", offset,data, dataOffset, len);
		   Assert.assertEquals(Ret,3*(1<<20));
		   tfsManager.fetchFile( userId, resourcesPath+"temp", "/text");
		   tfsManager.rmFile( userId, "/text");
	    }
	

}