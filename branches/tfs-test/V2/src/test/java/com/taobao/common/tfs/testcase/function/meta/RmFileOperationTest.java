package com.taobao.common.tfs.testcase.function.meta;

import java.io.FileNotFoundException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class RmFileOperationTest extends BaseCase {
	@Test
	public void testRm1KFile() {
		log.info("begin: " + getCurrentFunctionName());
		
		testRmFile("1K.jpg", "/rm_file_1K");

		log.info("end: " + getCurrentFunctionName());
	}
	

	@Test
	public void testRm2MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testRmFile("2M.jpg", "/rm_file_2M");
		
		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testRm3MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testRmFile("3M.jpg", "/rm_file_3M");

		log.info("end: " + getCurrentFunctionName());
	}

	// @Test
	public void testRm6GFile() {
		log.info("begin: " + getCurrentFunctionName());

		testRmFile("6G.jpg", "/rm_file_6G");

		log.info("end: " + getCurrentFunctionName());
	}
	
	private void testRmFile(String localFile,String tfsName){
		String filePath = tfsName + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);

		log.debug("@@@!! localFile: " + localFile + "; filePath: " + filePath
				+ "; " + "oldUsedCapacity:" + oldUsedCapacity + ";"
				+ "oldFileCount:" + oldFileCount);
		
		long localFileSize = 0;
		try {
			localFileSize = FileUtility.getFileSize(localFile);
		} catch (FileNotFoundException e) {
			log.debug("@@!! " + e.getMessage());
			Assert.assertTrue(false);
		}

		boolean result = tfsManager
				.saveFile(appId, userId, localFile, filePath);
		metaFiles.add(filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("@@@!!first sessionId: " + sessionId);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == localFileSize);

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);

		log.debug("@@@!! first sessionId: " + sessionId + "; "
				+ "getUsedCapacity:" + newUsedCapacity + "; FileCount:"
				+ newFileCount);

		Assert.assertEquals(oldUsedCapacity + localFileSize, newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@@@!! second sessionId: " + sessionId);

		int rmRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertEquals(rmRet, 0);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 4) == localFileSize);

		result = tfsManager.fetchFile(appId, userId, "temp", filePath);
		Assert.assertFalse(result);

		tfsManager.destroy();
	}
}
