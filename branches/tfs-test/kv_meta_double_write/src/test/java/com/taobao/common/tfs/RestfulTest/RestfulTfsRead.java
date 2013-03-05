package com.taobao.common.tfs.RestfulTest;


import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;



public class RestfulTfsRead extends RestfulTfsBaseCase 
{
	@Test
    public void test_01_read_right() throws FileNotFoundException
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
     
	   String File_name = "textread00001";
	   
       tfsManager.rmFile(appId, userId, "/"+File_name);
	   
	   tfsManager.saveFile(appId, userId, resourcesPath+"2B.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,2));
	   
	   FileInputStream input = new FileInputStream(resourcesPath+"2B.jpg");
	   System.out.println("@@@input: "+input);
	   
       OutputStream output = new FileOutputStream(resourcesPath+"Crc2b.jpg");
	   Assert.assertEquals(2,tfsManager.read(appId, userId, "/"+File_name, 0, 2, output));
	   
	   GetObject(buecket_name,File_name,TempFile);
       
       long localcrc;
       long readcrc;
       long kvmetacrc;
       localcrc=getCrc(resourcesPath+"2B.jpg");
       readcrc=getCrc(resourcesPath+"Crc2b.jpg");  
       kvmetacrc = getCrc(TempFile);
	   Assert.assertEquals(localcrc,readcrc);
	   Assert.assertEquals(localcrc,kvmetacrc);
	   
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	   
    }
	
	@Test
    public void test_02_read_right_large() throws FileNotFoundException
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   
	   String File_name = "textread2rightlarge";
	   
       tfsManager.rmFile(appId, userId, "/"+File_name); 
	   tfsManager.saveFile(appId, userId, resourcesPath+"100M.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<20)));
	   
	   int localcrc;
	   int readcrc;
	   long kvmetacrc;
	   localcrc=getCrc(resourcesPath+"100M.jpg");
	   OutputStream output = new FileOutputStream(resourcesPath+"Crc100M.jpg");
       long temp = tfsManager.read(appId, userId,"/"+File_name, 0, 100*(1<<20), output);
       System.out.println(temp+"!!!!!!!!!");
	   Assert.assertEquals(100*(1<<20),temp);
	   readcrc=getCrc(resourcesPath+"Crc100M.jpg");
	   
	   GetObject(buecket_name,File_name,TempFile);
	   kvmetacrc=getCrc(TempFile);
	   Assert.assertEquals(localcrc,readcrc);
	   Assert.assertEquals(localcrc,kvmetacrc);
	   
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
    }
	
	@Test
    public void test_03_read_with_offset()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   boolean Ret;
	   String File_name = "text3";
	   Ret=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
	   Assert.assertTrue(Ret);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
	   
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   long read;
	   read=tfsManager.read(appId, userId,"/"+File_name, 0, 98*( 1<<10), output);
	   Assert.assertEquals(98*(1<<10),read);
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
    }

	@Test
    public void test_04_read_more_offset()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   String File_name = "text";
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
	   
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(0,tfsManager.read(appId, userId,"/"+File_name, 1024*100+100, 100*( 1<<10), output));
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
    }
	
	@Test
    public void test_05_read_wrong_offset()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   String File_name = "text";
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
	   
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "/"+File_name, -1, 100*(1<<10), output)<0);
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
    }
	
	@Test
    public void test_06_read_null_output()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   String File_name = "text";
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
	   
	   Assert.assertTrue(tfsManager.read(appId, userId, "/"+File_name, 0, 100*(1<<10), null)<0);
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
    }

	@Test
    public void test_07_read_null_filePath()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, null, 0, 100*(1<<10), output)<0);
    }

	@Test
	public void test_08_read_empty_filePath()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId,"", 0, 100*( 1<<10), output)<0);
    }

	@Test
    public void test_09_read_wrong_filePath_1()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   String File_name = "text";
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
	   
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "text", 0, 100*(1<<10), output)<0);
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
    }

	@Test
    public void test_10_read_wrong_filePath_2()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   String File_name = "textreadwrong10";
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
	   
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(100*(1<<10),tfsManager.read(appId, userId, "///textreadwrong10///", 0, 100*(1<<10), output));
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
    }

	@Test
    public void test_11_read_not_exist()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "/text", 0, 100*(1<<10), output)<0);
    }
	@Test
    public void test_12_read_Dir()
	{
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.createDir(appId, userId, "/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "/text", 0, 100*(1<<10), output)<0);
	   tfsManager.rmDir(appId, userId, "/text");
	   
    }
}
