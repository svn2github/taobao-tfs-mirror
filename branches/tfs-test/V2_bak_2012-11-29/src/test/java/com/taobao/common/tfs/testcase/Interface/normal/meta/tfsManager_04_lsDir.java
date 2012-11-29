package com.taobao.common.tfs.testcase.Interface.normal.meta;

import java.util.ArrayList;
import java.util.List;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;


import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.testcase.metaTfsBaseCase;


public class tfsManager_04_lsDir extends metaTfsBaseCase 
{
	 @Before
	 public void Before()
	 {
		 int i,j;
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.createDir(appId, userId, "/textlsDir"+i);
			 tfsManager.createFile(appId, userId, "/textlsDirFile"+i);
		 }
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.createDir(appId, userId, "/textlsDir"+i+"/textlsDir"+j);
			tfsManager.createFile(appId, userId, "/textlsDir"+i+"/textlsDirFile"+j);
		 }
	 }
 @After
	 public void After()
	 {
		 int i,j;
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.rmDir(appId, userId, "/textlsDir"+i+"/textlsDir"+j);
			tfsManager.rmFile(appId, userId, "/textlsDir"+i+"/textlsDirFile"+j);
		 }
		 
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.rmDir(appId, userId, "/textlsDir"+i);
			 tfsManager.rmFile(appId, userId, "/textlsDirFile"+i);
		 }
		 
	 }
	 @Test 
	 public void test_01_lsDir_right_filePath()
	 {
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		FileMetaInfo metaInfo;
		int num=10;
		metaInfoList=tfsManager.lsDir(appId, userId, "/textlsDir1");
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
		log.info(new Throwable().getStackTrace()[0].getMethodName());
	}
	 @Test 
	 public void test_02_lsDir_null_filePath()
	 {
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir(appId, userId, null);
	    Assert.assertNull(metaInfoList);
	 }
	 @Test 
	 public void test_03_lsDir_empty_filePath()
	 {
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir(appId, userId, "");
	    Assert.assertNull(metaInfoList);
	 }
	 @Test 
	 public void test_04_lsDir_wrong_filePath_1()
	 {
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir(appId, userId, "textlsDir1");
	    Assert.assertNull(metaInfoList);
	 }
	 @Test 
	 public void test_05_lsDir_wrong_filePath_2()
	 {
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		FileMetaInfo metaInfo;
		int num=10;
		metaInfoList=tfsManager.lsDir(appId, userId, "///textlsDir1///");
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
		log.info(new Throwable().getStackTrace()[0].getMethodName());
	 }

	 @Test 
	 public void test_06_lsDir_not_exist_filePath()
	 {
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir(appId, userId, "/textlsDir1/textlsDir");
	    Assert.assertNull(metaInfoList);
	 }
	 
	 @Test 
	 public void test_08_lsDir_root()
	 {
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		metaInfoList=tfsManager.lsDir(appId, 73, "/");
	    Assert.assertNull(metaInfoList);
	 }
}
