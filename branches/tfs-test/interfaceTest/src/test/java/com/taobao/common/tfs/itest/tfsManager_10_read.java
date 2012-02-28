package com.taobao.common.tfs.itest;


import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;


public class tfsManager_10_read extends tfsNameBaseCase 
{
	@Test
    public void test_01_read_right() throws FileNotFoundException
	{
	   log.info("test_01_read_right");
	   tfsManager.createFile(appId, userId, "/text1");
	   tfsManager.saveFile(appId, userId, resourcesPath+"2b","/text1");
	   int localcrc;
	   int readcrc;
	   localcrc=getCrc(resourcesPath+"2b");
	   FileInputStream input = new FileInputStream(resourcesPath+"2b");
	   System.out.println(input);
	   
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(2,tfsManager.read(appId, userId, "/text1", 0, 2, output));
	   readcrc=getCrc(output);
	   
	   System.out.println(output.toByteArray());
	   Assert.assertEquals(localcrc,readcrc);
	   tfsManager.rmFile(appId, userId, "/text1");
    }
	@Test
    public void test_02_read_right_large() throws FileNotFoundException
	{
	   log.info("test_02_read_right_large");
	   tfsManager.createFile(appId, userId, "/text2");
	   tfsManager.saveFile(appId, userId, resourcesPath+"1G.jpg","/text2");
	   int localcrc;
	   int readcrc;
	   localcrc=getCrc(resourcesPath+"1G.jpg");
	   OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
	   Assert.assertEquals(1*(1<<30),tfsManager.read(appId, userId,"/text2", 0, 1*( 1<<30), output));
	   //tfsManager.fetchFile(appId, userId, resourcesPath+"TEMP", "/text");
	   readcrc=getCrc(resourcesPath+"empty.jpg");
	   Assert.assertEquals(localcrc,readcrc);
	   tfsManager.rmFile(appId, userId, "/text2");
    }
	@Test
    public void test_03_read_with_offset()
	{
	   log.info("test_03_read_with_offset");
	   boolean Ret;
	   tfsManager.createFile(appId, userId, "/text3");
	   Ret=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text3");
	   Assert.assertTrue(Ret);
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   long read;
	   read=tfsManager.read(appId, userId,"/text3", 0, 98*( 1<<10), output);
	   System.out.println("$$$$$"+read+"$$$$$$");
	   Assert.assertEquals(98*(1<<10),read);
	   tfsManager.rmFile(appId, userId, "/text3");
    }
	@Test
    public void test_04_read_more_offset()
	{
	   log.info("test_04_read_more_offset");
	   tfsManager.createFile(appId, userId, "/text");
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(0,tfsManager.read(appId, userId,"/text", 1024*100+100, 100*( 1<<10), output));
	   tfsManager.rmFile(appId, userId, "/text");
    }
	@Test
    public void test_05_read_wrong_offset()
	{
	   log.info("test_05_read_wrong_offset");
	   tfsManager.createFile(appId, userId, "/text");
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "/text", -1, 100*(1<<10), output)<0);
	   tfsManager.rmFile(appId, userId, "/text");
    }
	@Test
    public void test_06_read_null_output()
	{
	   log.info("test_06_read_null_output");
	   tfsManager.createFile(appId, userId, "/text");
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   Assert.assertTrue(tfsManager.read(appId, userId, "/text", 0, 100*(1<<10), null)<0);
	   tfsManager.rmFile(appId, userId, "/text");
    }
	@Test
    public void test_07_read_null_filePath()
	{
	   log.info("test_07_read_null_filePath");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, null, 0, 100*(1<<10), output)<0);
    }
	@Test
    public void test_08_read_empty_filePath()
	{
	   log.info("test_08_read_empty_filePath");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId,"", 0, 100*( 1<<10), output)<0);
    }
	@Test
    public void test_09_read_wrong_filePath_1()
	{
	   log.info("test_09_read_wrong_filePath_1");
	   tfsManager.createFile(appId, userId, "/text");
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "text", 0, 100*(1<<10), output)<0);
	   tfsManager.rmFile(appId, userId, "/text");
    }
	@Test
    public void test_10_read_wrong_filePath_2()
	{
	   log.info("test_10_read_wrong_filePath_2");
	   tfsManager.createFile(appId, userId, "/text");
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(100*(1<<10),tfsManager.read(appId, userId, "///text///", 0, 100*(1<<10), output));
	   tfsManager.rmFile(appId, userId, "/text");
    }
	//@Test  this case wrong use the 02
    public void test_11_read_large()
	{
	   log.info("test_11_read_large");
	   tfsManager.createFile(appId, userId, "/text");
	   tfsManager.saveFile(appId, userId, resourcesPath+"10M.jpg","/text");
	   long Ret;
	   int localcrc;
	   int readcrc;
	   localcrc=getCrc(resourcesPath+"10M.jpg");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Ret=tfsManager.read(appId, userId, "/text", 0,10*(1<<20), output);
	   System.out.println("!!!!!!!!!!!!!!!!"+Ret);
	   Assert.assertEquals(10*(1<<20),Ret);
	   readcrc=getCrc(output);
	   Assert.assertEquals(localcrc,readcrc);
	   tfsManager.rmFile(appId, userId, "/text");
    }
	@Test
    public void test_12_read_not_exist()
	{
	   log.info("test_12_read_not_exist");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "/text", 0, 100*(1<<10), output)<0);
    }
	@Test
    public void test_13_read_Dir()
	{
	   log.info("test_13_read_Dir");
	   tfsManager.createDir(appId, userId, "/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "/text", 0, 100*(1<<10), output)<0);
	   tfsManager.rmDir(appId, userId, "/text");
	   
    }
}
