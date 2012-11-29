package com.taobao.common.tfs.testcase.function.meta;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class OperationInfoTest extends BaseCase {
	@Test
	public void testWriteManyTimeParts() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile1B = "1B.jpg";
		String localFile10K = "10K.jpg";
		String filePath = "/pwirte_10K" + currentDateTime();

		Object[][] fileInfos = { { localFile10K, 0, true },
				{ localFile10K, 10 * (1 << 10), true },
				{ localFile10K, 20 * (1 << 10) + 1, true },
				{ localFile1B, 20 * (1 << 10), true },
				{ localFile1B, 20 * (1 << 10), false } };

		testWriteManyTimes(filePath, fileInfos);

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testWriteLargeManyTimesParts() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile1B = "1B.jpg";
		String localFile3M = "3M.jpg";
		String filePath = "/pwirte_3M" + currentDateTime();

		Object[][] fileInfos = { { localFile3M, 0, true },
				{ localFile3M, 3 * (1 << 20), true },
				{ localFile3M, 6 * (1 << 20) + 1, true },
				{ localFile1B, 6 * (1 << 20), true },
				{ localFile1B, 6 * (1 << 20), false } };

		testWriteManyTimes(filePath, fileInfos);

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testWriteManyTimesPartsCom() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile10K = "10K.jpg";
		String localFile2M = "2M.jpg";
		String localFile3M = "3M.jpg";
		String filePath = "/pwirte_10K" + currentDateTime();

		Object[][] fileInfos = { { localFile10K, 0, true },
				{ localFile2M, 20 * (1 << 10), true },
				{ localFile3M, 8 * (1 << 20) + 20 * (1 << 10), true },
				{ localFile10K, 10 * (1 << 10), true },
				{ localFile10K, 10 * (1 << 10), false },
				{ localFile3M, 2 * (1 << 20) + 20 * (1 << 10), true }, };

		testWriteManyTimes(filePath, fileInfos);

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testReadOperationInfo() {
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "100M.jpg";
		String filePath = "/pwrite_100M" + currentDateTime();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();

		tfsManager.rmFile(appId, userId, filePath);
		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);

		boolean result = tfsManager
				.saveFile(appId, userId, localFile, filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);

		String sessionId = tfsManager.getSessionId();
		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2),
				100 * (1 << 20));

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20), newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		ByteArrayOutputStream output = new ByteArrayOutputStream();

		long totalReadLength = 0;
		long readLength;
		long len = 5 * 1024;
		long dataOffset = 0;
		for (int i = 0; i < 5; i++) {

			readLength = tfsManager.read(appId, userId, filePath, dataOffset,
					len, output);
			Assert.assertEquals(readLength, len);

			totalReadLength += readLength;
			dataOffset += readLength;
			TimeUtility.sleep(MAX_STAT_TIME);
			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20),
					newUsedCapacity);
		}
		for (int i = 0; i < 5; i++) {
			readLength = tfsManager.read(appId, userId, "/unknown", dataOffset,
					len, output);
			Assert.assertTrue(readLength < 0);
			TimeUtility.sleep(MAX_STAT_TIME);
			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20),
					newUsedCapacity);
		}
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testWriteLargeFileInfo() throws FileNotFoundException {
		log.info("begin: " + getCurrentFunctionName());

		boolean result = false;
		String localFile = "100M.jpg";
		String filePath = "/pwite_100M" + currentDateTime();

		TfsStatus tfsStatus = new TfsStatus();

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		// long oldFileCount = tfsStatus.getFileCount(appKey);

		tfsManager = createTfsManager();
		tfsManager.rmFile(appId, userId, filePath);

		result = tfsManager.saveFile(appId, userId, localFile, filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);

		String sessionId = tfsManager.getSessionId();
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 100 * (1 << 20));
		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		// long newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20), newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		OutputStream output = new FileOutputStream("empty.jpg");

		long totalReadLength = 0;
		long readLength;
		long len = 90 * (1 << 20);
		long dataOffset = 0;
		for (int i = 0; i < 5; i++) {

			readLength = tfsManager.read(appId, userId, filePath, dataOffset,
					len, output);
			Assert.assertEquals(readLength, len);

			totalReadLength += readLength;
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			// newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20),
					newUsedCapacity);
		}
		for (int i = 0; i < 5; i++) {
			readLength = tfsManager.read(appId, userId, "/unknown", dataOffset,
					len, output);
			Assert.assertTrue(readLength < 0);
			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == totalReadLength);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			// newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 100 * (1 << 20),
					newUsedCapacity);
		}
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSaveSmallFileInfo() {
		log.info("begin: " + getCurrentFunctionName());

		boolean bRet = false;
		String localFile = "2M.jpg";
		String filePath = "/savesmallfile2M" + currentDateTime();

		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager.rmFile(appId, userId, filePath);

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		// long oldFileCount = tfsStatus.getFileCount(appKey);
		long newUsedCapacity = 0;
		// long newFileCount = 0;

		String sessionId = tfsManager.getSessionId();
		for (int i = 0; i < 5; i++) {

			bRet = tfsManager.saveFile(appId, userId, localFile, filePath + "_"
					+ i);
			Assert.assertTrue(bRet);

			TimeUtility.sleep(MAX_STAT_TIME);

			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == (i + 1) * 2 * 1024 * 1024);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			// newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + (i + 1) * 2 * 1024 * 1024,
					newUsedCapacity);
			// Assert.assertEquals(oldFileCount + i + 1, newFileCount);
		}

		localFile = "unknown";
		for (int i = 1; i < 6; i++) {
			TimeUtility.sleep(1);
			bRet = tfsManager.saveFile(appId, userId, localFile, filePath + "_"
					+ i);
			Assert.assertFalse(bRet);
			TimeUtility.sleep(MAX_STAT_TIME);

			// Assert.assertTrue(getOperTimes(sessionId, 2) == 5+i);//????
			// Assert.assertTrue(getSuccTimes(sessionId, 2) == 5);
			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == 5 * 2 * 1024 * 1024);
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			// newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 5 * 2 * 1024 * 1024,
					newUsedCapacity);
		}
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testUnlinkFile() {
		log.info("begin: " + getCurrentFunctionName());

		boolean bRet = false;
		int Ret;
		String localFile = "2M.jpg";
		String filePath = "/operation_" + currentDateTime();

		long oldUsedCapacity = 0;
		// long oldFileCount = 0;
		long newUsedCapacity = 0;
		// long newFileCount = 0;

		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();

		oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		// oldFileCount = tfsStatus.getFileCount(appKey);

		String sessionId = tfsManager.getSessionId();
		String[] sRet = new String[5];

		for (int i = 0; i < 5; i++) {
			TimeUtility.sleep(1);
			sRet[i] = filePath + "_" + i;
			bRet = tfsManager.saveFile(appId, userId, localFile, sRet[i]);
			Assert.assertTrue(bRet);
			TimeUtility.sleep(MAX_STAT_TIME);
			// Assert.assertTrue(getOperTimes(sessionId, 2) == i + 1);
			// Assert.assertTrue(getSuccTimes(sessionId, 2) == i + 1);
			Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == (i + 1)
					* 2 * (1 << 20));
			newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			// newFileCount = tfsStatus.getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + (i + 1) * 2 * (1 << 20),
					newUsedCapacity);
			// Assert.assertEquals(oldFileCount + i + 1, newFileCount);
		}
		tfsManager.destroy();

		tfsManager = createTfsManager();
		oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		// oldFileCount = tfsStatus.getFileCount(appKey);
		sessionId = tfsManager.getSessionId();
		for (int i = 0; i < 5; i++) {
			Ret = tfsManager.rmFile(appId, userId, sRet[i]);
			Assert.assertEquals(Ret, 0);

			if (i < 3) {
				Ret = tfsManager.rmFile(appId, userId, sRet[i]);
				Assert.assertTrue(Ret < 0);
			}
		}
		Ret = tfsManager.rmFile(appId, userId, "unknown1");
		Assert.assertEquals(Ret, -14010);
		Ret = tfsManager.rmFile(appId, userId, "unknown2");
		Assert.assertEquals(Ret, -14010);

		TimeUtility.sleep(MAX_STAT_TIME);
		// Assert.assertTrue(getOperTimes(sessionId, 4) == 10);
		// Assert.assertTrue(getSuccTimes(sessionId, 4) == 5);
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 4) == 5 * 2 * 1024 * 1024);
		newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		// newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity - 5 * 2 * (1 << 20),
				newUsedCapacity);
		// Assert.assertEquals(oldFileCount-5, newFileCount);
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	private void testWriteManyTimes(String filePath, Object[][] fileInfos) {
		TfsStatus tfsStatus = new TfsStatus();
		tfsManager = createTfsManager();

		tfsManager.rmFile(appId, userId, filePath);
		long ret = tfsManager.createFile(appId, userId, filePath);
		Assert.assertEquals(ret, 0);

		byte data[] = null;
		long fileLength = 0;
		long oldUsedCapacity = 0;
		for (Object[] fileInfo : fileInfos) {
			tfsManager = createTfsManager();
			oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
			try {
				data = FileUtility.getByte((String) fileInfo[0]);
				fileLength = FileUtility.getFileSize((String) fileInfo[0]);
			} catch (IOException e) {
				e.printStackTrace();
				Assert.assertTrue(false);
			}

			ret = tfsManager.write(appId, userId, filePath,
					((Integer) fileInfo[1]).intValue(), data, 0, data.length);
			TimeUtility.sleep(MAX_STAT_TIME);

			log.debug("filepath is: " + filePath + "; ret=" + ret);

			if (((Boolean) fileInfo[2]).booleanValue()) {
				Assert.assertEquals(fileLength, ret);
				Assert.assertEquals(oldUsedCapacity + fileLength,
						tfsStatus.getUsedCapacity(appKey));
			} else {
				Assert.assertTrue(ret < 0);
				Assert.assertEquals(oldUsedCapacity,
						tfsStatus.getUsedCapacity(appKey));
			}
			tfsManager.destroy();
		}
	}

}
