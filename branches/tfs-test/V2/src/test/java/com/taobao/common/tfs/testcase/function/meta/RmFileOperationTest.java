package com.taobao.common.tfs.testcase.function.meta;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class RmFileOperationTest extends BaseCase {
	@Test
	public void testRm1KFile() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "1K.jpg";
		String filePath = "/rm_file_1K" + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();

		tfsManager.rmFile(appId, userId, filePath);

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

		int rmRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertEquals(rmRet, 0);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 4) == 1024);

		result = tfsManager.fetchFile(appId, userId, "temp", filePath);
		Assert.assertFalse(result);

		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testRm2MFile() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "2M.jpg";
		String filePath = "/rm_file_2M" + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

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

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 2 * (1 << 20));

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);

		log.debug("@@@!! first sessionId: " + sessionId + "; "
				+ "getUsedCapacity:" + newUsedCapacity + "; FileCount:"
				+ newFileCount);

		Assert.assertEquals(oldUsedCapacity + 2 * (1 << 20), newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@@@!! second sessionId: " + sessionId);

		int rmRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertEquals(rmRet, 0);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 4) == 2 * (1 << 20));

		result = tfsManager.fetchFile(appId, userId, "temp", filePath);
		Assert.assertFalse(result);

		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testRm3MFile() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "3M.jpg";
		String filePath = "/rm_file_3M" + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

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

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 3 * (1 << 20));

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);

		log.debug("@@@!! first sessionId: " + sessionId + "; "
				+ "getUsedCapacity:" + newUsedCapacity + "; FileCount:"
				+ newFileCount);

		Assert.assertEquals(oldUsedCapacity + 3 * (1 << 20), newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@@@!! second sessionId: " + sessionId);

		int rmRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertEquals(rmRet, 0);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 4) == 3 * (1 << 20));

		result = tfsManager.fetchFile(appId, userId, "temp", filePath);
		Assert.assertFalse(result);

		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	// @Test
	public void testRm6GFile() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "6G.jpg";
		String filePath = "/rm_file_6G" + currentDateTime();
		long expectedSize = (long) (6 * ((long) 1 << 30));

		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

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

		Assert.assertEquals(expectedSize, tfsStatus.getFileSize(sessionId, 2));

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);

		log.debug("@@@!! first sessionId: " + sessionId + "; "
				+ "getUsedCapacity:" + newUsedCapacity + "; FileCount:"
				+ newFileCount);

		Assert.assertEquals(oldUsedCapacity + expectedSize, newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		log.debug("@@@!! second sessionId: " + sessionId);

		int rmRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertEquals(rmRet, 0);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertEquals(expectedSize, tfsStatus.getFileSize(sessionId, 4));

		result = tfsManager.fetchFile(appId, userId, "temp", filePath);
		Assert.assertFalse(result);

		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}
}
