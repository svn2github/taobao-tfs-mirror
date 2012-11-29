package com.taobao.common.tfs.testcase.function.rc;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class OperationInfoTest extends rcTfsBaseCase {

	@Test
	public void testReadOperationInfo() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "100M.jpg";
		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);

		String tfsName = tfsManager.saveLargeFile(localFile, null, null);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();

		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),
				100 * (1 << 20));

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);

		Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20), newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();

		byte[] data = new byte[5 * 1024];
		long totalReadLength = 0;
		for (int i = 0; i < 5; i++) {
			int fd = -1;
			fd = tfsManager.openReadFile(tfsName, null);
			Assert.assertTrue(fd > 0);
			int readLength = tfsManager.readFile(fd, data, 0, (i + 1) * 1024);
			Assert.assertTrue(readLength == (i + 1) * 1024);
			totalReadLength += readLength;
			String tfsFileName = tfsManager.closeFile(fd);
			Assert.assertNotNull(tfsFileName);
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20),
					newUsedCapacity);
			Assert.assertEquals(oldFileCount + 1, newFileCount);
		}
		for (int i = 0; i < 5; i++) {
			int fd = -1;
			fd = tfsManager.openReadFile("unknown", null);
			Assert.assertTrue(fd < 0);
			int readLength = tfsManager.readFile(fd, data, 0, 2 * 1024);
			Assert.assertTrue(readLength < 0);
			String tfsFileName = tfsManager.closeFile(fd);
			tfsManager.closeFile(fd);
			Assert.assertNull(tfsFileName);
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20),
					newUsedCapacity);
			Assert.assertEquals(oldFileCount + 1, newFileCount);
		}
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testWriteLargeFileOperationInfo() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "100M.jpg";

		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);

		String tfsName = tfsManager.saveLargeFile(localFile, null, null);
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();

		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),
				100 * (1 << 20));

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20), newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);

		DefaultTfsManager tfsManager1 = createTfsManager();
		sessionId = tfsManager1.getSessionId();
		byte[] data = new byte[90 * 1024 * 1024];
		long totalWriteLength = 0;
		for (int i = 0; i < 5; i++) {
			int fd = -1;
			fd = tfsManager.openReadFile(tfsName, null);
			Assert.assertTrue(fd > 0);
			int readLength = tfsManager.readFile(fd, data, 0, (i + 80)
					* (1 << 20));
			Assert.assertTrue(readLength == (i + 80) * (1 << 20));
			String tfsFileName = tfsManager.closeFile(fd);
			Assert.assertNotNull(tfsFileName);
			fd = -1;
			fd = tfsManager1.openWriteFile(null, null, "localfile");
			Assert.assertTrue(fd > 0);
			long writeLength = tfsManager1.writeFile(fd, data, 0, (i + 80)
					* (1 << 20));
			Assert.assertTrue(writeLength == (i + 80) * (1 << 20));
			totalWriteLength += writeLength;
			String writedTfsFileName = tfsManager1.closeFile(fd);
			Assert.assertNotNull(writedTfsFileName);
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),
					totalWriteLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20)
					+ totalWriteLength, newUsedCapacity);
			Assert.assertEquals(oldFileCount + i + 2, newFileCount);
		}

		localFile = "unknown";
		for (int i = 1; i < 6; i++) {
			tfsName = null;
			tfsName = tfsManager.saveLargeFile(localFile, null, null);
			Assert.assertNull(tfsName);
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == totalWriteLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20)
					+ totalWriteLength, newUsedCapacity);
			Assert.assertEquals(oldFileCount + 6, newFileCount);
		}

		tfsManager.destroy();
		tfsManager1.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSaveSmallFileOperationInfo() {

		log.info("begin: " + getCurrentFunctionName());

		String localFile = "2M.jpg";
		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();

		long newUsedCapacity, newFileCount;
		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);
		String sessionId = tfsManager.getSessionId();
		log.debug("sessionId is: " + sessionId);

		String tfsName = null;
		for (int i = 0; i < 5; i++) {
			tfsName = tfsManager.saveFile(localFile, null, null, false);
			tfsNames.add(tfsName);
			Assert.assertNotNull(tfsName);
			TimeUtility.sleep(MAX_STAT_TIME);
			Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2), (i + 1)
					* 2 * (1 << 20));

			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + (i + 1) * 2 * (1 << 20),
					newUsedCapacity);
			Assert.assertEquals(oldFileCount + i + 1, newFileCount);
		}

		localFile = "unknown";
		for (int i = 1; i < 6; i++) {
			TimeUtility.sleep(1);
			tfsName = tfsManager.saveFile(localFile, null, null, false);
			Assert.assertNull(tfsName);
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 5 * 2 * 1024 * 1024);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 5 * 2 * 1024 * 1024,
					newUsedCapacity);
			Assert.assertEquals(oldFileCount + 5, newFileCount);
		}
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testUnlinkFileOperationInfo() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "2M.jpg";

		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();
		String sessionId = tfsManager.getSessionId();

		long oldUsedCapacity, oldFileCount, newUsedCapacity, newFileCount;
		oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		oldFileCount = tfsStatus.getFileCount(appKey);

		String[] names = new String[5];
		for (int i = 0; i < 5; i++) {
			TimeUtility.sleep(1);
			names[i] = tfsManager.saveFile(localFile, null, null, false);
			tfsNames.add(names[i]);
			Assert.assertNotNull(names[i]);
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2), (i + 1)
					* 2 * (1 << 20));

			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + (i + 1) * 2 * (1 << 20),
					newUsedCapacity);
			Assert.assertEquals(oldFileCount + i + 1, newFileCount);
		}
		tfsManager.destroy();

		boolean result = false;
		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		for (int i = 0; i < 5; i++) {
			result = tfsManager.unlinkFile(names[i], null);
			Assert.assertTrue(result);
			if (i < 3) {
				result = tfsManager.unlinkFile(names[i], null);
				Assert.assertFalse(result);
			}
		}
		result = tfsManager.unlinkFile("unknown1", null);
		Assert.assertFalse(result);
		result = tfsManager.unlinkFile("unknown2", null);
		Assert.assertFalse(result);
		TimeUtility.sleep(MAX_STAT_TIME);

		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 4),
				5 * 2 * (1 << 20));

		newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
		Assert.assertEquals(oldFileCount, newFileCount);

		tfsManager.destroy();
		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testMixOperationInfo() {
		log.info("begin: " + getCurrentFunctionName());

		boolean result = false;
		String localFile = "2M.jpg";
		DefaultTfsManager tfsManager = createTfsManager();
		String[] tfsFiles = new String[10];
		// small files
		log.debug("=== smallfile ");
		for (int i = 0; i < 2; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveFile(localFile, null, null, false);
			Assert.assertNotNull(tfsFiles[i]);
			result = tfsManager.fetchFile(tfsFiles[i], null, "localfile");
			Assert.assertTrue(result);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
		}
		// fetch after unlink
		for (int i = 2; i < 4; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveFile(localFile, null, null, false);
			Assert.assertNotNull(tfsFiles[i]);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
			result = tfsManager.fetchFile(tfsFiles[i], null, "localfile");
			Assert.assertFalse(result);
		}
		// unlink twice
		for (int i = 0; i < 2; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveFile(localFile, null, null, false);
			Assert.assertNotNull(tfsFiles[i]);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertFalse(result);
		}

		log.debug("=== largefile ");

		localFile = "100M.jpg";
		// large file
		for (int i = 4; i < 6; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveLargeFile(localFile, null, null);
			Assert.assertNotNull(tfsFiles[i]);
			log.debug("=== savelarge " + i + " " + tfsFiles[i]);
			result = tfsManager.fetchFile(tfsFiles[i], null, "localfile");
			Assert.assertTrue(result);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
		}
		// fetch after unlink
		for (int i = 6; i < 8; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveLargeFile(localFile, null, null);
			Assert.assertNotNull(tfsFiles[i]);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
			result = tfsManager.fetchFile(tfsFiles[i], null, "localfile");
			Assert.assertFalse(result);
		}
		// unlink twice
		for (int i = 6; i < 8; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveLargeFile(localFile, null, null);
			Assert.assertNotNull(tfsFiles[i]);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertFalse(result);
		}
		// read file
		byte[] data = new byte[4 * 1024 * 1024];
		for (int i = 8; i < 10; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveLargeFile(localFile, null, null);
			Assert.assertNotNull(tfsFiles[i]);
			int fd = -1;
			fd = tfsManager.openReadFile(tfsFiles[i], null);
			Assert.assertTrue(fd > 0);
			int length = tfsManager.readFile(fd, data, 0, 4 * 1024 * 1021);
			Assert.assertTrue(length == 4 * 1024 * 1021);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
		}
		// read file after unlink
		for (int i = 8; i < 10; i++) {
			TimeUtility.sleep(1);
			tfsFiles[i] = tfsManager.saveLargeFile(localFile, null, null);
			Assert.assertNotNull(tfsFiles[i]);
			log.debug("@ tfsname: " + tfsFiles[i]);
			result = tfsManager.unlinkFile(tfsFiles[i], null);
			Assert.assertTrue(result);
			int fd = -1;
			fd = tfsManager.openReadFile(tfsFiles[i], null);
			Assert.assertTrue(fd < 0);
			int length = tfsManager.readFile(fd, data, 0, 4 * 1024 * 1021);
			Assert.assertTrue(length < 0);
		}
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

}
