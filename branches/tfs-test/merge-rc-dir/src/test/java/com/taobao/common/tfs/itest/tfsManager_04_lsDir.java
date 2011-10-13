package com.taobao.common.tfs.itest;

import java.util.ArrayList;
import java.util.List;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.function.tfsNameBaseCase;
import com.taobao.common.tfs.namemeta.FileMetaInfo;


public class tfsManager_04_lsDir extends tfsNameBaseCase 
{
	 @Before
	 public void Before()
	 {
		 int i,j;
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.createDir( userId, "/text"+i);
			 tfsManager.createFile( userId, "/textFile"+i);
		 }
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.createDir( userId, "/text"+i+"/text"+j);
			tfsManager.createFile( userId, "/text"+i+"/textFile"+j);
		 }
	 }
	 @After
	 public void After()
	 {
		 int i,j;
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.rmDir( userId, "/text"+i+"/text"+j);
			tfsManager.rmFile( userId, "/text"+i+"/textFile"+j);
		 }
		 
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.rmDir( userId, "/text"+i);
			 tfsManager.rmFile( userId, "/textFile"+i);
		 }
		 
	 }
	 @Test 
	    public void test_01_lsDir_right_filePath()//ÐÞ¸Änum=11
	    {
	      log.info( "test_01_lsDir_right_filePath" );
	      List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
	      metaInfoList=null;
	      FileMetaInfo metaInfo;
	      int num=10;
	      metaInfoList=tfsManager.lsDir( userId, "/text1");
	       Assert.assertNotNull(metaInfoList);//why
	       System.out.println("metaInfoList.size()°¡°¡°¡°¡\n"+metaInfoList.size());
	      Assert.assertEquals(num, metaInfoList.size());
	      int i;
	      for (i=0;i<num;i++)
	      {
	         log.info( "The"+i+"file" );
	         metaInfo=metaInfoList.get(i);
	         log.info( "The fileName is"+metaInfo.getFileName());
	         log.info( "The pid is"+metaInfo.getPid());
	         log.info( "The id is"+metaInfo.getId());
	         log.info( "The length is"+metaInfo.getLength());
	         log.info( "*****************************************************" );
	      }
	      log.info( "test_01_lsDir_right_filePath" );}

	 //@Test 
	 public void test_001_lsDir_right_filePath()
	 {
		log.info( "test_01_lsDir_right_filePath" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		FileMetaInfo metaInfo;
		int num=11;
		metaInfoList=tfsManager.lsDir( userId, "/text1");
	    Assert.assertNotNull(metaInfoList);//why
	   // System.out.println("metaInfoList.size()°¡°¡°¡°¡\n"+metaInfoList.size());
		Assert.assertEquals(num, metaInfoList.size());
		int i;
		for (i=0;i<num;i++)
		{
		   log.info( "The"+i+"file" );
		   metaInfo=metaInfoList.get(i);
		   log.info( "The fileName is"+metaInfo.getFileName());
		   log.info( "The pid is"+metaInfo.getPid());
		   log.info( "The id is"+metaInfo.getId());
		   log.info( "The length is"+metaInfo.getLength());
		   log.info( "*****************************************************" );
		}
		log.info( "test_01_lsDir_right_filePath" );
	}
	 @Test 
	 public void test_02_lsDir_null_filePath()
	 {
		log.info( "test_02_lsDir_null_filePath" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir( userId, null);
	    Assert.assertNull(metaInfoList);
	 }
	 @Test 
	 public void test_03_lsDir_empty_filePath()
	 {
		log.info( "test_03_lsDir_empty_filePath" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir( userId, "");
	    Assert.assertNull(metaInfoList);
	 }
	 @Test 
	 public void test_04_lsDir_wrong_filePath_1()
	 {
		log.info( "test_04_lsDir_wrong_filePath_1" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir( userId, "text1");
	    Assert.assertNull(metaInfoList);
	 }
	 @Test 
	 public void test_05_lsDir_wrong_filePath_2()
	 {
		log.info( "test_05_lsDir_wrong_filePath_2" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		FileMetaInfo metaInfo;
		int num=10;
		metaInfoList=tfsManager.lsDir( userId, "///text1///");
	    Assert.assertNotNull(metaInfoList);//why
		Assert.assertEquals(num, metaInfoList.size());
		int i;
		for (i=0;i<num;i++)
		{
		   log.info( "The"+i+"file" );
		   metaInfo=metaInfoList.get(i);
		   log.info( "The fileName is"+metaInfo.getFileName());
		   log.info( "The pid is"+metaInfo.getPid());
		   log.info( "The id is"+metaInfo.getId());
		   log.info( "The length is"+metaInfo.getLength());
		   log.info( "*****************************************************" );
		}
		log.info( "test_05_lsDir_wrong_filePath_2" );
	 }
	 @Test 
	 public void test_06_lsDir_empty_filePath()
	 {
		log.info( "test_06_lsDir_empty_filePath" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
//		FileMetaInfo metaInfo;
		metaInfoList=tfsManager.lsDir( userId, " ");
//		System.out.println(metaInfoList.size());
	    Assert.assertNull(metaInfoList);
	 }
	 @Test 
	 public void test_07_lsDir_not_exist_filePath()
	 {
		log.info( "test_07_lsDir_not_exist_filePath" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir( userId, "/text1/text");
	    Assert.assertNull(metaInfoList);
	 }
}