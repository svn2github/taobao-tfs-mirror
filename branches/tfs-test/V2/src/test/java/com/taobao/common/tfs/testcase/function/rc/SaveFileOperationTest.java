package com.taobao.common.tfs.testcase.function.rc;

import java.io.FileNotFoundException;
import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class SaveFileOperationTest extends BaseCase {
	
	@Test
	public void testSave1KFile(){
		log.info("begin: " + getCurrentFunctionName());
		
		testSaveFile("1K.jpg");
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testSave2MFile(){
		log.info("begin: " + getCurrentFunctionName());
		
		testSaveFile("2M.jpg");
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testSave3MFile(){
		log.info("begin: " + getCurrentFunctionName());
		
		testSaveFile("3M.jpg");
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testSave6GFile(){
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("6G.jpg");
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testUpdateFile10KTo100K(){
		log.info("begin: " + getCurrentFunctionName());
		
	    String localFile = "10K.jpg";
	    TfsStatus tfsStatus = new TfsStatus();
	    DefaultTfsManager tfsManager = createTfsManager();
	    
	    TimeUtility.sleep(MAX_STAT_TIME);
	    long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    long oldFileCount = tfsStatus.getFileCount(appKey);
	    
	    String tfsName = tfsManager.saveFile(localFile,null,null, false);
	    Assert.assertNotNull(tfsName);
	    TimeUtility.sleep(MAX_STAT_TIME);
	    
	    String sessionId = tfsManager.getSessionId();
	    log.debug("file size: "+tfsStatus.getFileSize(sessionId, 2));
	    Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),10*1024);
	    
	    long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    long newFileCount = tfsStatus.getFileCount(appKey);
	    Assert.assertEquals(oldUsedCapacity + 10*1024, newUsedCapacity);
	    Assert.assertEquals(oldFileCount + 1, newFileCount);
	    tfsManager.destroy();

	    localFile = "100K.jpg";
	    tfsManager = createTfsManager();
	   
	    String tfsName1=tfsManager.saveFile(localFile, tfsName, null, false);
	    Assert.assertEquals(tfsName1, tfsName);
	    TimeUtility.sleep(MAX_STAT_TIME);
	    sessionId = tfsManager.getSessionId();
	    
	    Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),100*1024);
	    newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    newFileCount = tfsStatus.getFileCount(appKey);
	    
	    Assert.assertEquals(oldUsedCapacity + 110*1024, newUsedCapacity);
	    Assert.assertEquals(oldFileCount + 2, newFileCount);
	    boolean result = tfsManager.fetchFile(tfsName1, null, "localfile");
	    Assert.assertTrue(result);

	    tfsManager.destroy();
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testUpdateFile100KTo10K(){
		log.info("begin: " + getCurrentFunctionName());
		
	    String localFile = "100K.jpg";
	    TfsStatus tfsStatus = new TfsStatus();
	    DefaultTfsManager tfsManager = createTfsManager();
	    
	    TimeUtility.sleep(MAX_STAT_TIME);
	    long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    long oldFileCount = tfsStatus.getFileCount(appKey);
	    
	    String tfsName = tfsManager.saveFile(localFile,null,null, false);
	    Assert.assertNotNull(tfsName);
	    TimeUtility.sleep(MAX_STAT_TIME);
	    
	    String sessionId = tfsManager.getSessionId();
	    log.debug("file size: "+tfsStatus.getFileSize(sessionId, 2));
	    Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),100*1024);
	    
	    long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    long newFileCount = tfsStatus.getFileCount(appKey);
	    Assert.assertEquals(oldUsedCapacity + 100*1024, newUsedCapacity);
	    Assert.assertEquals(oldFileCount + 1, newFileCount);
	    tfsManager.destroy();

	    localFile = "10K.jpg";
	    tfsManager = createTfsManager();
	   
	    String tfsName1=tfsManager.saveFile(localFile, tfsName, null, false);
	    Assert.assertEquals(tfsName1, tfsName);
	    TimeUtility.sleep(MAX_STAT_TIME);
	    sessionId = tfsManager.getSessionId();
	    
	    Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),10*1024);
	    newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    newFileCount = tfsStatus.getFileCount(appKey);
	    
	    Assert.assertEquals(oldUsedCapacity + 110*1024, newUsedCapacity);
	    Assert.assertEquals(oldFileCount + 2, newFileCount);
	    boolean result = tfsManager.fetchFile(tfsName1, null, "localfile");
	    Assert.assertTrue(result);

	    tfsManager.destroy();
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	private void testSaveFile(String localFile){
		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();
		
		TimeUtility.sleep(MAX_STAT_TIME);
		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long fileLength = 0;
		try {
			fileLength = FileUtility.getFileSize(localFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
		 
		String tfsName = "";
		if(fileLength>(2*(1<<20))){
			tfsName = tfsManager.saveLargeFile(localFile, null, null);
		}else{
			tfsName = tfsManager.saveFile(localFile, null, null,false);
		}
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.debug("Saved tfsname is: "+tfsName);
		
		TimeUtility.sleep(MAX_STAT_TIME);
	    String sessionId = tfsManager.getSessionId();
	    log.debug("sessionId: " + sessionId);
	    
	    Assert.assertEquals(fileLength, tfsStatus.getFileSize(sessionId, 2));
	    long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
	    
	    Assert.assertEquals(oldUsedCapacity + fileLength, newUsedCapacity);
	    tfsManager.destroy();
	    
	    tfsManager = createTfsManager();
	    sessionId = tfsManager.getSessionId();
	    boolean result = tfsManager.fetchFile(tfsName, null, "localfile");
	    Assert.assertTrue(result);
	    
	    TimeUtility.sleep(MAX_STAT_TIME);
	    log.debug("expected fileLength is: "+fileLength+"; actual get size is: "+tfsStatus.getFileSize(sessionId, 1));
	    Assert.assertEquals(fileLength, tfsStatus.getFileSize(sessionId, 1));
	    
	    tfsManager.destroy();
	}
}
