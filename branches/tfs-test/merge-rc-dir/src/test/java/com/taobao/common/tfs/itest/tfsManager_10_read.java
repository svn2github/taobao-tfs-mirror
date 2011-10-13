package com.taobao.common.tfs.itest;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.function.tfsNameBaseCase;


public class tfsManager_10_read extends tfsNameBaseCase 
{
	@Test
    public void test_01_01_read_right() throws FileNotFoundException
	{
	   log.info("test_01_read_right");
	   tfsManager.createFile(userId, "/text");
	   tfsManager.saveFile( userId,resourcesPath+"2b","/text");
	   int localcrc;
	   int readcrc;
	   localcrc=getCrc(resourcesPath+"2b");
	   FileInputStream input = new FileInputStream(resourcesPath);
	   System.out.println(input);
	   
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(2,tfsManager.read( userId, "/text", 0, 2, output));
	   readcrc=getCrc(output);
	   
	   System.out.println(output.toByteArray());
	   Assert.assertEquals(localcrc,readcrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_01_read_right_1() throws FileNotFoundException
	{
	   log.info("test_01_read_right_1");
	   tfsManager.createFile( userId, "/text");
	   tfsManager.saveFile( userId, resourcesPath+"1G","/text");
	   int localcrc;
	   int readcrc;
	   localcrc=getCrc(resourcesPath+"1G");
	   OutputStream output = new FileOutputStream(resourcesPath+"empty.jpg");
	   Assert.assertEquals(1*(1<<30),tfsManager.read( userId,"/text", 0, 1*( 1<<30), output));
	   //tfsManager.fetchFile( userId, resourcesPath+"TEMP", "/text");
	   readcrc=getCrc(resourcesPath+"empty.jpg");
	   Assert.assertEquals(localcrc,readcrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_02_read_with_offset()
	{
	   log.info("test_02_read_with_offset");
	   boolean Ret;
	   tfsManager.createFile( userId, "/text");
	   Ret=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
	   Assert.assertTrue(Ret);
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   long read;
	   read=tfsManager.read( userId,"/text", 0, 98*( 1<<10), output);
	   System.out.println("$$$$$"+read+"$$$$$$");
	   Assert.assertEquals(99*(1<<10),read);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_03_read_more_offset()
	{
	   log.info("test_03_read_more_offset");
	   tfsManager.createFile( userId, "/text");
	   tfsManager.saveFile( userId, resourcesPath+"100K","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(0,tfsManager.read( userId,"/text", 1024*100+100, 100*( 1<<10), output));
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_04_read_wrong_offset()
	{
	   log.info("test_03_read_more_offset");
	   tfsManager.createFile( userId, "/text");
	   tfsManager.saveFile( userId, resourcesPath+"100K","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read( userId, "/text", -1, 100*(1<<10), output)<0);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_05_read_null_output()
	{
	   log.info("test_05_read_null_output");
	   tfsManager.createFile( userId, "/text");
	   tfsManager.saveFile( userId, resourcesPath+"100K","/text");
	   Assert.assertTrue(tfsManager.read( userId, "/text", 0, 100*(1<<10), null)<0);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_06_read_null_filePath()
	{
	   log.info("test_06_read_null_filePath");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read( userId, null, 0, 100*(1<<10), output)<0);
    }
	@Test
    public void test_07_read_empty_filePath()
	{
	   log.info("test_07_read_empty_filePath");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read( userId,"", 0, 100*( 1<<10), output)<0);
    }
	@Test
    public void test_08_read_wrong_filePath_1()
	{
	   log.info("test_08_read_wrong_filePath_1");
	   tfsManager.createFile( userId, "/text");
	   tfsManager.saveFile( userId, resourcesPath+"100K","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read( userId, "text", 0, 100*(1<<10), output)<0);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_09_read_wrong_filePath_2()
	{
	   log.info("test_08_read_wrong_filePath_1");
	   tfsManager.createFile( userId, "/text");
	   tfsManager.saveFile( userId, resourcesPath+"100K","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(100*(1<<10),tfsManager.read( userId, "///text///", 0, 100*(1<<10), output));
	   tfsManager.rmFile( userId, "/text");
    }
	//@Test
    public void test_10_read_large()
	{
	   log.info("test_10_read_large");
	   tfsManager.createFile( userId, "/text");
	   tfsManager.saveFile( userId, resourcesPath+"1G","/text");
	   int localcrc;
	   int readcrc;
	   localcrc=getCrc(resourcesPath+"1G");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(1<<30,tfsManager.read( userId, "/text", 0,1<<30, output));
	   readcrc=getCrc(output);
	   Assert.assertEquals(localcrc,readcrc);
	   tfsManager.rmFile( userId, "/text");
    }
	@Test
    public void test_11_read_not_exist()
	{
	   log.info("test_11_read_not_exist");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read( userId, "/text", 0, 100*(1<<10), output)==0);
    }
	@Test
    public void test_12_read_Dir()
	{
	   log.info("test_12_read_Dir");
	   tfsManager.createDir( userId, "/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read( userId, "/text", 0, 100*(1<<10), output)==0);
	   tfsManager.rmDir( userId, "/text");
    }
}