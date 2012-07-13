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


public class tfsManager_05_lsDir_isRecursive extends metaTfsBaseCase 
{
	@Before
	 public void BeforeClass()
	 {
         String str = "/lsDirtest";
         int Ret;
         Ret = tfsManager.createDir(appId, userId, str);
         Assert.assertEquals(Ret,0);
		 int i,j,k;
		 int n=0;
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.createDir(appId, userId, str + "/text"+i);
			 tfsManager.createFile(appId, userId, str + "/textFile"+i);
			 ++n;
		 }
		 System.out.println("!!!!!!!!!!!!!!!!!!!!!! 5 n="+n);
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.createDir(appId, userId, str + "/text"+i+"/text"+j);
			tfsManager.createFile(appId, userId, str + "/text"+i+"/textFile"+j);
			++n;
		 }
		 System.out.println("!!!!!!!!!!!!!!!!!!!!!! 30 n="+n);
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
	     for(k=1;k<=5;k++)
		 {
			tfsManager.createDir(appId, userId, str + "/text"+i+"/text"+j+"/text"+k);
			tfsManager.createFile(appId, userId, str + "/text"+i+"/text"+j+"/textFile"+k);
			++n;
		 }		 
		 
		 System.out.println("!!!!!!!!!!!!!!!!!!!!!! 155 n="+n);
	 }
	 @After
	 public void AfterClass()
	 {
         String str = "/lsDirtest";
		 int i,j,k,n=0;
		 	 
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 for(k=1;k<=5;k++)
		 {
			tfsManager.rmDir(appId, userId, str + "/text"+i+"/text"+j+"/text"+k);
			tfsManager.rmFile(appId, userId, str + "/text"+i+"/text"+j+"/textFile"+k);
			n++;
		 }
		 for(i=1;i<=5;i++)
		 for(j=1;j<=5;j++)
		 {
			tfsManager.rmDir(appId, userId, str + "/text"+i+"/text"+j);
			tfsManager.rmFile(appId, userId, str + "/text"+i+"/textFile"+j);
            n++;
		 }
		 
		 for(i=1;i<=5;i++)
		 {
			 tfsManager.rmDir(appId, userId, str + "/text"+i);
			 tfsManager.rmFile(appId, userId, str + "/textFile"+i);
             n++;
		 }
         System.out.println("************155 n="+n);
		 tfsManager.rmDir(appId, userId, str);
	 }
	 @Test 
	 public void test_01_lsDir_right_isRecursive_filePath()
	 {
		log.info( "test_01_lsDir_right_isRecursive_filePath" );
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList=null;
		FileMetaInfo metaInfo;
		int num=310;
		metaInfoList=tfsManager.lsDir(appId, userId, "/lsDirtest",true);
	    Assert.assertNotNull(metaInfoList);//why
	    System.out.println("!!!!!!!!!!!!!!!!metaInfoList.size:"+metaInfoList.size());
		Assert.assertEquals(num, metaInfoList.size());
		
		int i;
		for (i=0;i<num;i++)
		{
		   log.info( "The "+i+" file" );
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
