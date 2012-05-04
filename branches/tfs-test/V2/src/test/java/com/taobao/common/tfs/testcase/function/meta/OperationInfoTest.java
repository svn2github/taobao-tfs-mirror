package com.taobao.common.tfs.testcase.function.meta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
				{ localFile3M, 8*(1<<20)+20*(1<<10), true },
				{ localFile10K, 10*(1<<10), true },
				{ localFile10K, 10*(1<<10), false },
				{ localFile3M, 2*(1<<20)+20*(1<<10), true },};

		testWriteManyTimes(filePath, fileInfos);
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testReadOperationInfo(){
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
	
	private void testWriteManyTimes(String filePath,Object[][] fileInfos){
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
