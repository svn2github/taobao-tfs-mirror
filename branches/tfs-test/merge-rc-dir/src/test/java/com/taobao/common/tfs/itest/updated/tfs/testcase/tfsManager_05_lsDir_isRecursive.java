package com.taobao.common.tfs.test.new.tfs.testcase;

import java.util.ArrayList;
import java.util.List;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;
import com.taobao.common.tfs.namemeta.FileMetaInfo;


public class tfsManager_05_lsDir_isRecursive extends tfsNameBaseCase 
{
	@Before
	 public void BeforeClass()
	 {
		 int i,j,k;
		 int n=0;
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.createDir(appId, userId, "/text"+i);
			 tfsManager.createFile(appId, userId, "/textFile"+i);
			 ++n;
		 }
		 System.out.println("!!!!!!!!!!!!!!!!!!!!!!"+n);
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.createDir(appId, userId, "/text"+i+"/text"+j);
			tfsManager.createFile(appId, userId, "/text"+i+"/textFile"+j);
			++n;
		 }
		 System.out.println("!!!!!!!!!!!!!!!!!!!!!!"+n);
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
	     for(k=1;k<=5;k++)
		 {
			tfsManager.createDir(appId, userId, "/text"+i+"/text"+j+"/text"+k);
			tfsManager.createFile(appId, userId, "/text"+i+"/text"+j+"/textFile"+k);
			++n;
		 }
		 
		 
		 System.out.println("!!!!!!!!!!!!!!!!!!!!!!"+n);
	 }
	 @After
	 public void AfterClass()
	 {
		 int i,j,k;
		 
		 
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 for(k=1;k<=5;k++)
		 {
			tfsManager.rmDir(appId, userId, "/text"+i+"/text"+j+"/text"+k);
			tfsManager.rmFile(appId, userId, "/text"+i+"/text"+j+"/textFile"+k);
			
		 }
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.rmDir(appId, userId, "/text"+i+"/text"+j);
			tfsManager.rmFile(appId, userId, "/text"+i+"/textFile"+j);
		 }
		 
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.rmDir(appId, userId, "/text"+i);
			 tfsManager.rmFile(appId, userId, "/textFile"+i);
		 }
		 
	 }
	 @Test 
	 public void test_01_lsDir_right_isRecursive_filePath()
	 {
		log.info( "test_01_lsDir_right_isRecursive_filePath" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		FileMetaInfo metaInfo;
		int num=310;
		metaInfoList=tfsManager.lsDir(appId, userId, "/",true);
	    Assert.assertNotNull(metaInfoList);//why
	    System.out.println("!!!!!!!!!!!!!!!!"+metaInfoList.size());
		Assert.assertEquals(num, metaInfoList.size());
		
		int i;
		for (i=0;i<num;i++)
		{
		   log.info( "The"+i+"file" );
		   metaInfo=metaInfoList.get(i);
		   log.info( "The fileName is "+metaInfo.getFileName());
		   log.info( "The pid is "+metaInfo.getPid());
		   log.info( "The id is "+metaInfo.getId());
		   log.info( "The length is "+metaInfo.getLength());
		   log.info( "*****************************************************" );
		}
		log.info( "test_01_lsDir_right_isRecursive_filePath" );
	}
}