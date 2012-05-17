package com.taobao.common.tfs.testcase.function.meta;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class SaveFileOperationTest extends BaseCase {

	@Test
	public void testSave1KFile() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "1K.jpg";
		String filePath = "/save_file_1K" + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);
		TimeUtility.sleep(MAX_STAT_TIME);
		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);

		log.debug("@@@!! localFile: " + localFile + "; filePath: " + filePath
				+ "; " + "oldUsedCapacity:" + oldUsedCapacity + ";"
				+ "oldFileCount:" + oldFileCount);

		boolean result = tfsManager
				.saveFile(appId, userId, localFile, filePath);
		metaFiles.add(filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("@@@!!first sessionId: " + sessionId);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 1024);

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);

		log.debug("@@@!! first sessionId: " + sessionId + "; "
				+ "getUsedCapacity:" + newUsedCapacity + "; FileCount:"
				+ newFileCount);

		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@@@!! second sessionId: " + sessionId);

		result = tfsManager.fetchFile(appId, userId, "localFile", filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == 1024);
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave2MFile() {

		log.info("begin: " + getCurrentFunctionName());

		String localFile = "2M.jpg";
		String filePath = "/save_file_2M" + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);
		boolean result = tfsManager
				.saveFile(appId, userId, localFile, filePath);
		metaFiles.add(localFile);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("@!! first sessionId: " + sessionId);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 2 * 1024 * 1024);

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 2 * 1024 * 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@!! second sessionId: " + sessionId);

		result = tfsManager.fetchFile(appId, userId, "localFile", filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == 2 * 1024 * 1024);
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave3MFile() {

		log.info("begin: " + getCurrentFunctionName());

		String localFile = "3M.jpg";
		String filePath = "/save_file_3M" + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);
		boolean result = tfsManager
				.saveFile(appId, userId, localFile, filePath);
		metaFiles.add(localFile);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("@!! first sessionId: " + sessionId);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 3 * 1024 * 1024);

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 3 * 1024 * 1024, newUsedCapacity);
		log.debug("@!! oldFileCount: " + oldFileCount + "; newFileCount: "
				+ newFileCount);

		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@!! second sessionId: " + sessionId);

		result = tfsManager.fetchFile(appId, userId, "localFile", filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == 3 * 1024 * 1024);
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}
}
