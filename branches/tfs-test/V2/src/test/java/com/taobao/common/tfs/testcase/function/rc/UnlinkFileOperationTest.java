package com.taobao.common.tfs.testcase.function.rc;

import java.io.FileNotFoundException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class UnlinkFileOperationTest extends rcTfsBaseCase {

	@Test
	public void testUnlinkFile1K() {
		log.info("begin: " + getCurrentFunctionName());

		testUnlinkFile("1K.jpg");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testUnlinkFile2M() {
		log.info("begin: " + getCurrentFunctionName());

		testUnlinkFile("2M.jpg");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testUnlinkFile3M() {
		log.info("begin: " + getCurrentFunctionName());

		testUnlinkFile("3M.jpg");

		log.info("end: " + getCurrentFunctionName());
	}

	// @Test
	public void testUnlinkFile6G() {
		log.info("begin: " + getCurrentFunctionName());

		testUnlinkFile("6G.jpg");

		log.info("end: " + getCurrentFunctionName());
	}

	private void testUnlinkFile(String localFile) {
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
		if (fileLength > (2 * (1 << 20))) {
			tfsName = tfsManager.saveLargeFile(localFile, null, null);
		} else {
			tfsName = tfsManager.saveFile(localFile, null, null, false);
		}
		tfsNames.add(tfsName);
		Assert.assertNotNull(tfsName);
		log.debug("Saved tfsname is: " + tfsName);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("sessionId: " + sessionId);

		Assert.assertEquals(fileLength, tfsStatus.getFileSize(sessionId, 2));
		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);

		Assert.assertEquals(oldUsedCapacity + fileLength, newUsedCapacity);
		tfsManager.destroy();

		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		boolean result = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		log.debug("expected fileLength is: " + fileLength
				+ "; actual get size is: "
				+ tfsStatus.getFileSize(sessionId, 1));
		Assert.assertEquals(fileLength, tfsStatus.getFileSize(sessionId, 4));

		newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		Assert.assertEquals(oldUsedCapacity, newUsedCapacity);

		result = tfsManager.fetchFile(tfsName, null, "localfile");
		Assert.assertFalse(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == 0);

		tfsManager.destroy();
	}
}
