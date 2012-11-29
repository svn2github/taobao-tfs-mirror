package com.taobao.common.tfs.testcase.function.meta;

import java.io.FileNotFoundException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class SaveFileOperationTest extends BaseCase {

	@Test
	public void testSave1KFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("1K.jpg", "/save_file_1K");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave2MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("2M.jpg", "/save_file_2M");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave3MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("3M.jpg", "/save_file_3M");

		log.info("end: " + getCurrentFunctionName());
	}
	
	//@Test
	public void testSave6GFile() {
		log.info("begin: " + getCurrentFunctionName());
		
		testSaveFile("6G.jpg", "/save_file_6G");

		log.info("end: " + getCurrentFunctionName());
	}

	private void testSaveFile(String localFile, String tfsFile) {
		String filePath = tfsFile + currentDateTime();

		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);

		long localFileSize = 0;
		try {
			localFileSize = FileUtility.getFileSize(localFile);
		} catch (FileNotFoundException e) {
			log.debug("@@!! " + e.getMessage());
			Assert.assertTrue(false);
		}

		boolean result = tfsManager
				.saveFile(appId, userId, localFile, filePath);
		metaFiles.add(localFile);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("@!! first sessionId: " + sessionId);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == localFileSize);

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + localFileSize, newUsedCapacity);
		log.debug("@!! oldFileCount: " + oldFileCount + "; newFileCount: "
				+ newFileCount);

		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@!! second sessionId: " + sessionId);

		result = tfsManager.fetchFile(appId, userId, "localFile", filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == localFileSize);
		tfsManager.destroy();
	}
}
