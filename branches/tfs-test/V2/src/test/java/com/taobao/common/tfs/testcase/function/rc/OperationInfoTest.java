package com.taobao.common.tfs.testcase.function.rc;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class OperationInfoTest extends BaseCase {

	@Test
	public void testReadOperationInfo() {
		log.info("begin: " + getCurrentFunctionName());
		
	    String localFile = "100M.jpg";
	    TfsStatus tfsStatus = new TfsStatus();
	    DefaultTfsManager tfsManager = createTfsManager();
	    
	    long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    long oldFileCount = tfsStatus.getFileCount(appKey);
	    
	    String tfsName=tfsManager.saveLargeFile(localFile,null,null);
	    tfsNames.add(tfsName);
	    Assert.assertNotNull(tfsName);
	    
	    TimeUtility.sleep(MAX_STAT_TIME);
	    String sessionId = tfsManager.getSessionId();
	    
	    Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),100*(1<<20));
	    
	    long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    long newFileCount = tfsStatus.getFileCount(appKey);
	    
	    Assert.assertEquals(oldUsedCapacity + 100*(1<<20), newUsedCapacity);
	    Assert.assertEquals(oldFileCount + 1, newFileCount);
	    tfsManager.destroy();
	    
	    tfsManager = createTfsManager();
	    sessionId = tfsManager.getSessionId();
	    
	    byte[] data = new byte[5*1024];
	    long totalReadLength = 0;
	    for(int i = 0; i < 5; i++)
	    {
	      int fd = -1;
	      fd = tfsManager.openReadFile(tfsName, null);
	      Assert.assertTrue(fd>0);
	      int readLength = tfsManager.readFile(fd, data, 0, (i+1)*1024);
	      Assert.assertTrue(readLength == (i+1)*1024);
	      totalReadLength += readLength;
	      String tfsFileName = tfsManager.closeFile(fd);
	      Assert.assertNotNull(tfsFileName);
	      TimeUtility.sleep(MAX_STAT_TIME);

	      Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
	      newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	      newFileCount = tfsStatus.getFileCount(appKey);
	      Assert.assertEquals(oldUsedCapacity + 100*(1<<20), newUsedCapacity);
	      Assert.assertEquals(oldFileCount + 1, newFileCount);
	    }
	    for(int i = 0; i < 5; i++)
	    {
	      int fd = -1;
	      fd = tfsManager.openReadFile("unknown", null);
	      Assert.assertTrue(fd<0);
	      int readLength = tfsManager.readFile(fd, data, 0, 2*1024);
	      Assert.assertTrue(readLength<0);
	      String tfsFileName = tfsManager.closeFile(fd);
	      tfsManager.closeFile(fd);
	      Assert.assertNull(tfsFileName);
	      TimeUtility.sleep(MAX_STAT_TIME);
	      
	      Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
	      newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	      newFileCount = tfsStatus.getFileCount(appKey);
	      Assert.assertEquals(oldUsedCapacity + 100*(1<<20), newUsedCapacity);
	      Assert.assertEquals(oldFileCount + 1, newFileCount);
	    }
	    tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

}
