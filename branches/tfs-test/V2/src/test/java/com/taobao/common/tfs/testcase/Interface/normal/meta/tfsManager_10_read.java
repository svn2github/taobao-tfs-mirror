package com.taobao.common.tfs.MetaITest_2_2_3;


import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;


public class tfsManager_10_read extends tfsNameBaseCase 
{
	@Test
    public void test_01_read_right() throws FileNotFoundException
	{
	   log.info("test_01_read_right");
       
       tfsManager.rmFile(appId, userId, "/textread00001");
	   
	   tfsManager.saveFile(appId, userId, resourcesPath+"2B.jpg","/textread00001");
       
	   FileInputStream input = new FileInputStream(resourcesPath+"2B.jpg");
	   System.out.println("@@@input: "+input);
	   
       OutputStream output = new FileOutputStream(resourcesPath+"Crc2b.jpg");
	   Assert.assertEquals(2,tfsManager.read(appId, userId, "/textread00001", 0, 2, output));
       
       long localcrc;
       long readcrc;
       localcrc=getCrc(resourcesPath+"2B.jpg");
       readcrc=getCrc(resourcesPath+"Crc2b.jpg");     	   
	   Assert.assertEquals(localcrc,readcrc);

	   tfsManager.rmFile(appId, userId, "/textread00001");
       
    }
	@Test
    public void test_02_read_right_large() throws FileNotFoundException
	{
	   log.info("test_02_read_right_large");
       tfsManager.rmFile(appId, userId, "/textread2rightlarge"); 
	   tfsManager.saveFile(appId, userId, resourcesPath+"1G.jpg","/textread2rightlarge");
	   int localcrc;
	   int readcrc;
	   localcrc=getCrc(resourcesPath+"1G.jpg");
	   OutputStream output = new FileOutputStream(resourcesPath+"Crc1G.jpg");
       long temp = tfsManager.read(appId, userId,"/textread2rightlarge", 0, 1*( 1<<30), output);
       System.out.println(temp+"!!!!!!!!!");
	   Assert.assertEquals(1*(1<<30),temp);
	   readcrc=getCrc(resourcesPath+"Crc1G.jpg");
	   Assert.assertEquals(localcrc,readcrc);
	   tfsManager.rmFile(appId, userId, "/textread2rightlarge");
    }
	@Test
    public void test_03_read_with_offset()
	{
	   log.info("test_03_read_with_offset");
	   boolean Ret;
	   Ret=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text3");
	   Assert.assertTrue(Ret);
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   long read;
	   read=tfsManager.read(appId, userId,"/text3", 0, 98*( 1<<10), output);
	   Assert.assertEquals(98*(1<<10),read);
	   tfsManager.rmFile(appId, userId, "/text3");
    }
	@Test
    public void test_04_read_more_offset()
	{
	   log.info("test_04_read_more_offset");
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(-14016,tfsManager.read(appId, userId,"/text", 1024*100+100, 100*( 1<<10), output));
	   tfsManager.rmFile(appId, userId, "/text");
    }
	@Test
    public void test_05_read_wrong_offset()
	{
	   log.info("test_05_read_wrong_offset");
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
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertTrue(tfsManager.read(appId, userId, "text", 0, 100*(1<<10), output)<0);
	   tfsManager.rmFile(appId, userId, "/text");
    }
	@Test
    public void test_10_read_wrong_filePath_2()
	{
	   log.info("test_10_read_wrong_filePath_2");
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textreadwrong10");
	   ByteArrayOutputStream output = new ByteArrayOutputStream();
	   Assert.assertEquals(100*(1<<10),tfsManager.read(appId, userId, "///textreadwrong10///", 0, 100*(1<<10), output));
	   tfsManager.rmFile(appId, userId, "/textreadwrong10");
    }
	//@Test  this case wrong use the 02
    public void test_11_read_large()
	{
	   log.info("test_11_read_large");
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
